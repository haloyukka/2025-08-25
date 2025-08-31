了解しました。  
以下は **「運用マニュアル風」** に整形したマークダウンまとめです。現場でそのまま手順書として利用できるよう、章立て・目的・実行手順・確認・注意事項を明示しています。

---

# 運用マニュアル

**SQL Server インデックス再構成・再構築手順**

---

## 1. 目的

- データベースのインデックス断片化を解消し、クエリ性能を安定化させる。
    
- 定期的なメンテナンスにより、長期的な性能劣化を防止する。
    

---

## 2. 対象環境

- **DBMS**: Microsoft SQL Server 2017 以降
    
- **権限**: `ALTER` 権限または `db_owner` 相当の権限が必要
    
- **Edition制約**: `ONLINE=ON` は Enterprise / EvCore エディションでのみ利用可能
    

---

## 3. 手順

### Step 1. 断片化状況の確認

以下のSQLを実行し、断片化率とページ数を確認する。

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

---

### Step 2. 再構成（REORGANIZE）

- 適用条件: **断片化率 5〜30%**
    
- 常にオンラインで実行可能
    

```sql
ALTER INDEX ALL ON dbo.YourTable
REORGANIZE WITH (LOB_COMPACTION = ON);
GO
```

---

### Step 3. 再構築（REBUILD）

- 適用条件: **断片化率 30%以上**
    
- ONLINE=ON は Enterprise/EvCore のみ利用可
    

```sql
ALTER INDEX ALL ON dbo.YourTable
REBUILD WITH (FILLFACTOR = 90, SORT_IN_TEMPDB = ON, ONLINE = ON);
GO
```

---

### Step 4. 統計情報の更新

- REORGANIZE 実行時は必須
    
- REBUILD 実行時は自動更新されるが、必要に応じ追加実行
    

```sql
UPDATE STATISTICS dbo.YourTable WITH FULLSCAN;
GO
EXEC sp_updatestats;
GO
```

---

### Step 5. 自動化（バッチスクリプト例）

以下のスクリプトで断片化率に応じて自動実行する。

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

## 4. 実施タイミング

- **推奨実行時間帯**: 夜間や低負荷帯
    
- **実施頻度**: 月次 or 四半期ごと（利用状況による）
    
- **特記事項**: 大規模テーブルは必ず事前にバックアップを取得
    

---

## 5. 確認方法

- Step 1 のスクリプトを再実行し、断片化率が改善されていることを確認する。
    

---

## 6. 注意事項

- **ONLINE オプション**: Standard Edition では利用不可。
    
- **FILLFACTOR**: 書き込み頻度が高いテーブルは 80〜90 を指定、参照主体なら 100 で問題なし。
    
- **TempDB 容量**: `SORT_IN_TEMPDB = ON` 利用時は TempDB の空き容量を要確認。
    
- **トランザクションログ**: REBUILD はログサイズが急増するため、監視・拡張設定が必要。
    

---

## 7. 参考情報（日本語サイト）

1. [Microsoft Docs: インデックスの再構成/再構築](https://learn.microsoft.com/ja-jp/sql/relational-databases/indexes/reorganize-and-rebuild-indexes?view=sql-server-ver17)
    
2. [Microsoft Docs: ALTER INDEX 構文](https://learn.microsoft.com/ja-jp/sql/t-sql/statements/alter-index-transact-sql?view=sql-server-ver17)
    
3. [Microsoft Docs: sys.dm_db_index_physical_stats](https://learn.microsoft.com/ja-jp/sql/relational-databases/system-dynamic-management-views/sys-dm-db-index-physical-stats-transact-sql?view=sql-server-ver17)
    
4. [Microsoft Docs: UPDATE STATISTICS](https://learn.microsoft.com/ja-jp/sql/relational-databases/statistics/update-statistics?view=sql-server-ver17)
    
5. [Microsoft Docs: sp_updatestats](https://learn.microsoft.com/ja-jp/sql/relational-databases/system-stored-procedures/sp-updatestats-transact-sql?view=sql-server-ver17)
    
6. [クラウドコンフィグ Tech Blog](https://tech-blog.cloud-config.jp/2022-07-13-sql-server-rebuild-index)
    
7. [＠IT／ITmedia: sys.dm_db_index_physical_stats 解説](https://atmarkit.itmedia.co.jp/ait/articles/2205/31/news001.html)
    
8. [ZOZO TECH BLOG: REORGANIZE vs REBUILD](https://techblog.zozo.com/entry/sqlserver-index-reorganize-vs-rebuild)
    
9. [Project Group: 統計情報とREBUILDの関係](https://www.projectgroup.info/tips/SQLServer/SQL/SQL000024.html)
    
10. [Netplanetes V2: sys.dm_db_index_physical_stats 使用例](https://www.pine4.net/Memo2/Article/Archive/153)
    

---

✅ **このマニュアルを印刷して運用チームに配布すれば、即座に定期メンテ手順として利用可能です。**

---

ご要望ですが、このマニュアルを **「チェックリスト形式（現場オペレーターがそのまま✔できる）」** に再構成しましょうか？