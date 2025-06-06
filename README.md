
# 🚀 Big Data Project – Vehicle Analysis (Job 1 & Job 3)

This repository contains Spark Cork , Spark SQL and MapReduce jobs focused on large-scale vehicle dataset processing.

## 📁 Contents

- `Job 1`: Brand-Model statistics (count, price range, years)
- `Job 3`: Grouping similar engine specs and finding average price and top model

## 🛠️ Technologies

- Apache Hadoop (MapReduce)
- Apache Spark Core (RDD API)
- Apache Spark SQL

## 📊 Performance Benchmark

Performance comparison on 2 million rows (local vs cluster):

| Job     | Technology     | Local Time | Cluster Time |
|---------|----------------|------------|---------------|
| Job 1   | MapReduce      | 135s       | 100s          |
|         | Spark Core     | 36.5s      | 21.3s         |
|         | Spark SQL      | 25.1s      | 17.0s         |
| Job 3   | MapReduce      | 480s       | 250s          |
|         | Spark Core     | 60s        | 30s           |
|         | Spark SQL      | 40s        | 19s           |

## 📂 Output

- Cleaned CSVs and output are stored in `/output/` folders per job
- Graphs are available for visual comparison




## 👤 Author

Bishoy Zakhary  
Progetto Big Data – Università Roma Tre  
