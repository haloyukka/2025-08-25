承知しました 👍  
ここまでの内容をマークダウン形式に整理しました。

---

# SQL Server インデックス再構成・再構築手順まとめ

## 手順概要

1. **断片化を確認**  
    `sys.dm_db_index_physical_stats` を用いてインデックスの断片化率を取得。
    
2. **再構成（REORGANIZE）**  
    軽度（目安：5〜30%）の断片化では再構成を実施。オンライン処理可能。
    
3. **再構築（REBUILD）**  
    重度（30%以上）の断片化では再構築を実施。必要に応じて `ONLINE=ON`、`FILLFACTOR` を設定。
    
4. **統計情報の更新**  
    再構築には統計更新が含まれるが、再構成のみの場合は `UPDATE STATISTICS` または `sp_updatestats` を実行。
    
5. **自動化スクリプト**  
    断片化率に応じて REORGANIZE / REBUILD を振り分け、バッチで実行。
    

---

## サンプルSQL

### 1. 断片化確認

```sql
USE YourDatabaseName;
GO
SELECT
  obj = OBJECT_SCHEMA_NAME(s.[object_id]) + '.' + OBJECT_NAME(s.[object_id]),
  ix  = i.name,
  s.index_id,
  s.avg_fragmentation_in_percent,
  s.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') s
JOIN sys.indexes i
  ON s.[object_id] = i.[object_id] AND s.index_id = i.index_id
WHERE i.type_desc IN ('CLUSTERED','NONCLUSTERED')
ORDER BY s.avg_fragmentation_in_percent DESC, s.page_count DESC;
```

### 2. 再構成（REORGANIZE）

```sql
ALTER INDEX ALL ON dbo.YourTable
REORGANIZE WITH (LOB_COMPACTION = ON);
GO
```

### 3. 再構築（REBUILD）

```sql
ALTER INDEX ALL ON dbo.YourTable
REBUILD WITH (FILLFACTOR = 90, SORT_IN_TEMPDB = ON, ONLINE = ON);
GO

ALTER INDEX IX_YourTable_ColA ON dbo.YourTable
REBUILD WITH (FILLFACTOR = 90, ONLINE = ON);
GO
```

### 4. 統計情報更新

```sql
UPDATE STATISTICS dbo.YourTable WITH FULLSCAN;
GO
EXEC sp_updatestats;
GO
```

### 5. 自動化スクリプト

```sql
USE YourDatabaseName;
GO
DECLARE @low float = 5.0, @mid float = 30.0;
DECLARE @sql nvarchar(max);

;WITH fr AS (
  SELECT [object_id], index_id, avg_fragmentation_in_percent, page_count
  FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED')
)
SELECT @sql = STRING_AGG(CMD, CHAR(10) + 'GO' + CHAR(10))
FROM (
  SELECT CASE
    WHEN f.avg_fragmentation_in_percent BETWEEN @low AND @mid THEN
      N'ALTER INDEX ' + QUOTENAME(i.name) + N' ON '
      + QUOTENAME(OBJECT_SCHEMA_NAME(f.[object_id])) + N'.' + QUOTENAME(OBJECT_NAME(f.[object_id]))
      + N' REORGANIZE WITH (LOB_COMPACTION = ON);'
    WHEN f.avg_fragmentation_in_percent >= @mid THEN
      N'ALTER INDEX ' + QUOTENAME(i.name) + N' ON '
      + QUOTENAME(OBJECT_SCHEMA_NAME(f.[object_id])) + N'.' + QUOTENAME(OBJECT_NAME(f.[object_id]))
      + N' REBUILD WITH (FILLFACTOR = 90, SORT_IN_TEMPDB = ON, ONLINE = ON);'
    ELSE NULL END AS CMD
  FROM fr f
  JOIN sys.indexes i
    ON i.[object_id] = f.[object_id] AND i.index_id = f.index_id
  WHERE i.type_desc IN ('CLUSTERED','NONCLUSTERED')
    AND f.page_count >= 1000
) x
WHERE CMD IS NOT NULL;

PRINT @sql;       -- 内容確認
EXEC sp_executesql @sql;  -- 実行
GO
EXEC sp_updatestats;
GO
```

---

## 運用ポイント

- **権限**: `ALTER` 権限や `db_owner` 権限が必要。
    
- **ONLINE オプション**: Enterprise/EvCore で利用可。Standard では利用不可。
    
- **FILLFACTOR**: 更新頻度が高い場合は 80〜90、参照主体なら 100 も可。
    
- **運用時間帯**: バッチ処理は夜間・低負荷帯に実施。`SORT_IN_TEMPDB` 使用時は TempDB 容量に注意。
    
- **検証**: メンテ後に再度断片化率を確認して効果をチェック。
    

---

## 出典（日本語サイト・URL）

1. [Microsoft Docs: インデックスの再構成/再構築](https://learn.microsoft.com/ja-jp/sql/relational-databases/indexes/reorganize-and-rebuild-indexes?view=sql-server-ver17)
    
2. [Microsoft Docs: ALTER INDEX 構文](https://learn.microsoft.com/ja-jp/sql/t-sql/statements/alter-index-transact-sql?view=sql-server-ver17)
    
3. [Microsoft Docs: sys.dm_db_index_physical_stats](https://learn.microsoft.com/ja-jp/sql/relational-databases/system-dynamic-management-views/sys-dm-db-index-physical-stats-transact-sql?view=sql-server-ver17)
    
4. [Microsoft Docs: UPDATE STATISTICS](https://learn.microsoft.com/ja-jp/sql/relational-databases/statistics/update-statistics?view=sql-server-ver17)
    
5. [Microsoft Docs: sp_updatestats](https://learn.microsoft.com/ja-jp/sql/relational-databases/system-stored-procedures/sp-updatestats-transact-sql?view=sql-server-ver17)
    
6. [クラウドコンフィグ Tech Blog: SQL Serverインデックス再構築](https://tech-blog.cloud-config.jp/2022-07-13-sql-server-rebuild-index)
    
7. [＠IT／ITmedia: sys.dm_db_index_physical_stats 解説](https://atmarkit.itmedia.co.jp/ait/articles/2205/31/news001.html)
    
8. [ZOZO TECH BLOG: REORGANIZE vs REBUILD 比較](https://techblog.zozo.com/entry/sqlserver-index-reorganize-vs-rebuild)
    
9. [Project Group: 統計情報とREBUILDの関係](https://www.projectgroup.info/tips/SQLServer/SQL/SQL000024.html)
    
10. [Netplanetes V2: sys.dm_db_index_physical_stats 使用例](https://www.pine4.net/Memo2/Article/Archive/153)
    

---

👉 このまとめをベースに、運用ポリシー（しきい値や実行時間帯、FILLFACTOR など）を自社環境に合わせて調整してください。

---

このマークダウンまとめをさらに **「運用マニュアル風」** に整えてみますか？