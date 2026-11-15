# Laporan Milestone 2a: Komparasi Kinerja PySpark

| Item | Keterangan |
|---|---|
| Nama | **[Rosan Kelvino Andre]** |
| NIM | **[42430025]** |
| Run ID | `smoke-a` |
| Mata kuliah | Analitik dan Skalabilitas Data (MK35) |
| Tanggal praktikum | 9 November 2026 |
| Repositori | **[https://github.com/RosanKelvino/Praktikum_7_AnalitikData]** |

## 1. Lingkungan pengujian

| Komponen | Nilai |
|---|---|
| CPU dan RAM laptop | **[isi: tipe CPU, jumlah core, RAM total]** |
| Docker | **[isi: versi Docker]**; RAM untuk Docker ±7,6 GiB (WSL2) |
| Redpanda | v26.2.5, 1 broker, topik `ecommerce.orders` 3 partisi, RF 1 |
| MinIO / Iceberg | MinIO; Iceberg 1.7.1 dengan REST Catalog |
| Spark | 3.5.3, Java 17, mode `local[4]`, driver memory 3g |
| Data input | 20.000 baris |
| Shuffle partition / salt bucket | 8 / 16 |
| AQE / autoBroadcastJoinThreshold | nonaktif / −1 |
| Pengulangan benchmark | 3 percobaan, dilaporkan dengan median |

Spark UI (Executors) menunjukkan 1 executor dengan 4 core, 205 task selesai, 0 gagal. Mode `local[4]` berarti empat thread pada satu mesin, bukan cluster.

## 2. Topologi dan aliran data

```
Producer Python -> Redpanda topic ecommerce.orders (3 partisi) -> Consumer Python
   -> MinIO bucket raw (JSONL) -> Spark (cleaning, percobaan A-E)
   -> Iceberg curated (orders_clean, sales_summary; REST Catalog + MinIO warehouse)
```

- Producer mengirim JSON dengan key `product_id`, `acks=all`.
- Consumer (group `lab-raw-smoke-a`) mengunggah batch JSONL ke bucket `raw`, lalu meng-commit offset (at-least-once; replay mungkin terjadi).
- Spark membaca `s3a://raw/events/run_id=smoke-a/...` dengan skema eksplisit.
- Hasil ditulis ke Iceberg dengan partisi `sale_date`.

## 3. Validasi dan kualitas data

| Tahap | Baris | Keterangan |
|---|---:|---|
| Data mentah | 20.000 | Event yang diterima consumer |
| Ditolak aturan cleaning | 40 | 21 city kosong + 20 amount null − 1 tumpang tindih |
| Duplikat dibuang | 0 | 20.000 − 40 = 19.960, tidak ada selisih lain |
| Data bersih | 19.960 | |
| Iceberg `orders_clean` | 19.960 | Sama dengan data bersih (`ICEBERG_OK`) |

Aturan cleaning dan alasannya:

- `order_id` tidak null: kunci deduplikasi.
- `city` tidak kosong: agregasi per kota hanya untuk kota valid.
- `sale_date` valid: menjadi kolom partisi Iceberg.
- `amount` tidak null dan ≥ 0: agar SUM benar.
- `quantity` > 0 dan `product_id` 1–200: transaksi bermakna dan ada di tabel dimensi.
- `dropDuplicates(run_id, order_id)`: mengatasi replay at-least-once.

## 4. Analisis execution plan dan shuffle

Percobaan A (`groupBy("city")`) memiliki satu shuffle:

```
HashAggregate (akhir)
+- Exchange hashpartitioning(city, 8)      <- shuffle
   +- HashAggregate (parsial)
      +- InMemoryTableScan                 <- data cache
```

- `groupBy` adalah wide transformation (butuh `Exchange`); `filter` dan `project` narrow (tanpa `Exchange`).
- Job 2: 0,6 s, 2 stage (1 dilewati), 16 task.
- Stage 6: 8 task; shuffle write 1862 B / 24 record (8 task × 3 kota); durasi task min 36 ms, median 0,2 s, maks 0,3 s.
- Agregasi parsial mengecilkan 19.960 baris menjadi 24 record sebelum shuffle, sehingga data yang dikocok sangat kecil.

## 5. Penanganan data skew

| Ukuran | Sebelum salting (B) | Sesudah salting (C) |
|---|---|---|
| Partisi kosong | 5 dari 8 | 0 dari 8 |
| Baris partisi terbesar | 15.968 (80%) | 5.611 (28,1%) |
| Skew index (maks ÷ rata-rata 2.495) | 6,40 | 2,25 |
| Exchange saat query | 0 | 1 |
| HashAggregate pada plan | 2 | 4 |
| Median waktu | 0,3977 s | 0,9897 s |

Baris per partisi, sebelum: `0, 0, 1996, 15968, 0, 0, 1996, 0`; sesudah: `2562, 1795, 5611, 4525, 245, 1232, 1396, 2594`.

- Hasil agregasi per kota identik pada A, B, dan C (`VALIDASI_SALTING_OK`).
- Pada B tidak ada `Exchange` saat query karena `repartition` terjadi saat cache dibuat. Task terlama hanya 69 ms (median 57 ms): 15.968 baris terlalu kecil untuk menjadi hambatan.
- Pada C, stage akhir hanya mengagregasi 3 kota sehingga sebagian besar dari 8 task kosong. Salting menambah dua tahap agregasi dan satu `Exchange`.
- Trade-off: partisi lebih merata (6,40 → 2,25) tetapi waktu lebih lambat (speedup 0,40x).

## 6. Broadcast join

| Aspek | D (SortMergeJoin) | E (BroadcastHashJoin) |
|---|---|---|
| Exchange saat query | 3 | 1 (agregasi) |
| Sort | 2 | 0 |
| BroadcastExchange | 0 | 1 |
| Job di Spark UI | Job 9: 0,8 s, 28 task | Job 10: 65 ms + Job 11: 0,3 s, 16 task |
| Median waktu | 1,2155 s | 0,6737 s |

- Pada D kedua sisi join dikocok dan diurutkan menurut `product_id`. Pada E tabel dimensi (200 baris) di-broadcast sehingga tabel transaksi tidak dikocok.
- Hasil identik (`VALIDASI_JOIN_OK`). `groupBy(category)` setelah join tetap menghasilkan satu `Exchange`.
- Batasan memori: tabel yang di-broadcast harus muat di memori driver dan setiap eksekutor; untuk tabel besar, SortMergeJoin lebih aman.

## 7. Partition pruning dan Iceberg

Tabel `orders_clean` dipartisi per `sale_date` (30 tanggal: 1–30 September 2026). Plan untuk `sale_date = 2026-09-05`:

```
BatchScan lake.curated.orders_clean
  [filters=sale_date IS NOT NULL, sale_date = 20701, groupedBy=]
```

- `20701` adalah 2026-09-05 dalam hari sejak 1 Januari 1970; filter diteruskan ke scan Iceberg.
- Hasil: 666 baris (667 transaksi pada tanggal itu dikurangi 1 yang ditolak cleaning).
- Count tanggal tersebut berdurasi 45 ms dengan plan `LocalTableScan` satu baris, yang mengindikasikan hasil dihitung dari metadata tanpa memindai file tanggal lain.
- Snapshot terbaru: **[isi snapshot_id terbaru]**. `createOrReplace` membuat snapshot baru pada setiap run.
- Keterbatasan: plan tidak menyebutkan jumlah file yang dilewati.

## 8. Benchmark komparatif

Waktu `collect()` dalam detik.

| Eksperimen | Percobaan 1 | Percobaan 2 | Percobaan 3 | Median | Temuan |
|---|---:|---:|---:|---:|---|
| GroupBy baseline (A) | 1,3571 | 1,5011 | 0,9619 | **1,3571** | Satu `Exchange`; memuat warm-up |
| Skew sebelum salting (B) | 0,2880 | 0,7136 | 0,3977 | **0,3977** | Tanpa `Exchange`; tercepat tetapi 80% baris di satu partisi |
| Skew setelah salting (C) | 1,1285 | 0,9897 | 0,6688 | **0,9897** | Partisi merata tetapi lebih lambat dari B |
| SortMergeJoin (D) | 1,2155 | 1,5192 | 1,0905 | **1,2155** | Dua `Exchange` dan dua `Sort` di sisi join |
| BroadcastHashJoin (E) | 0,6737 | 0,9706 | 0,6153 | **0,6737** | Tanpa shuffle dan sort di sisi join |

Perubahan (%) = (baseline − optimasi) ÷ baseline × 100%; speedup = baseline ÷ optimasi.

| Perbandingan | Median baseline | Median optimasi | Perubahan | Speedup | Per percobaan |
|---|---:|---:|---:|---:|---|
| Salting (B → C) | 0,3977 | 0,9897 | −148,9% | 0,40x | 0,26x / 0,72x / 0,59x |
| Broadcast (D → E) | 1,2155 | 0,6737 | +44,6% | 1,80x | 1,80x / 1,57x / 1,77x |

- Variasi antar percobaan besar (rentang 0,36–0,54 s), jadi median dipakai dan angka tidak boleh dibaca sebagai mutlak.
- A tidak dibandingkan dengan percobaan lain karena query-nya berbeda.
- Waktu yang diukur hanya `collect()`; `repartition`, `persist`, dan penulisan Iceberg tidak termasuk.

## 9. Kesimpulan dan catatan keamanan

- **Broadcast join berhasil:** plan berubah ke BroadcastHashJoin dan median turun 44,6% (1,80x), konsisten di ketiga percobaan.
- **Salting tidak mempercepat pada skala ini:** partisi lebih merata (skew index 6,40 → 2,25) dan hasil identik, tetapi waktu naik (0,40x). Ketimpangan 15.968 baris terlalu kecil untuk menjadi hambatan, sedangkan salting menambah dua tahap agregasi dan satu `Exchange`.
- **Local mode ≠ cluster:** `local[4]` adalah satu mesin tanpa jaringan antar-node. Pada cluster, shuffle lebih mahal sehingga manfaat broadcast dan salting bisa lebih besar. Kesimpulan tidak dapat digeneralisasi.
- **Keterbatasan:** satu broker (RF 1) tidak membuktikan failover; data sintetis dan kecil (20.000 baris); tabel Iceberg dibuat ulang dengan `createOrReplace` pada tiap run.
- **Keamanan:** kredensial `labadmin` / `labpassword123` hanya dummy untuk lab lokal; broker dan MinIO tanpa TLS; produksi memerlukan secret manager, enkripsi, kontrol akses, dan strategi idempotensi.

## Lampiran

- Repositori: **[https://github.com/RosanKelvino/Praktikum_7_AnalitikData/tree/main]**
- Berkas bukti: `hasil/run1`, `hasil/run2`, `hasil/run3` (`quality.json`, `benchmarks.csv`, `partition_sizes.csv`, `plan_*.txt`).
- Screenshot Spark UI, MinIO, dan terminal ada pada laporan Word (`Laporan_Milestone_2a.docx`).
