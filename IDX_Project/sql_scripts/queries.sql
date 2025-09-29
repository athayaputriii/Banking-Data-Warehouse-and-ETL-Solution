CREATE DATABASE DWH;

USE DWH;

-- DimAccount
CREATE TABLE DimAccount (
    AccountID INT PRIMARY KEY,
    CustomerID INT,
    AccountType VARCHAR(50),
    Balance DECIMAL(15,2),
    DateOpened DATE,
    Status VARCHAR(20)
);

CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    Address VARCHAR(200),
    CityName VARCHAR(50),
    StateName VARCHAR(50),
    Age INT,
    Gender VARCHAR(10),
    Email VARCHAR(100)
);

-- DimBranch
CREATE TABLE DimBranch (
    BranchID INT PRIMARY KEY,
    BranchName VARCHAR(100),
    BranchLocation VARCHAR(200)
);

-- FactTransaction
CREATE TABLE FactTransaction (
    TransactionID INT PRIMARY KEY,
    AccountID INT FOREIGN KEY REFERENCES DimAccount(AccountID),
    TransactionDate DATETIME,
    Amount DECIMAL(15,2),
    TransactionType VARCHAR(50),
    BranchID INT FOREIGN KEY REFERENCES DimBranch(BranchID)
);

-- Lihat isi tabel-tabel sample
SELECT TOP 5 * FROM sample.dbo.account;
SELECT TOP 5 * FROM sample.dbo.customer;
SELECT TOP 5 * FROM sample.dbo.transaction_db;
SELECT TOP 5 * FROM sample.dbo.branch;
SELECT TOP 5 * FROM sample.dbo.city;
SELECT TOP 5 * FROM sample.dbo.state;


-- Jalankan di SSMS untuk verifikasi
USE DWH;

-- Cek jumlah data di setiap tabel
SELECT 
    'DimBranch' as TableName, COUNT(*) as TotalRecords FROM DimBranch
UNION ALL
SELECT 'DimAccount', COUNT(*) FROM DimAccount
UNION ALL  
SELECT 'DimCustomer', COUNT(*) FROM DimCustomer
UNION ALL
SELECT 'FactTransaction', COUNT(*) FROM FactTransaction;

-- Sample data dari setiap tabel
SELECT * FROM DimBranch;
SELECT  * FROM DimAccount;
SELECT  * FROM DimCustomer;
SELECT  * FROM FactTransaction;

--stored procedure, daily transaction
USE DWH;
GO

CREATE PROCEDURE DailyTransaction
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SELECT 
        CAST(TransactionDate AS DATE) as Date,
        COUNT(*) as TotalTransactions,
        SUM(Amount) as TotalAmount
    FROM FactTransaction
    WHERE CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date
    GROUP BY CAST(TransactionDate AS DATE)
    ORDER BY Date;
END;
GO

EXEC DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-22';

--stored procedure, balance per customer
USE DWH;
GO

CREATE PROCEDURE BalancePerCustomer
    @name VARCHAR(100)
AS
BEGIN
    WITH CustomerTransactions AS (
        SELECT 
            c.CustomerName,
            a.AccountType,
            a.Balance,
            ft.Amount,
            ft.TransactionType,
            CASE 
                WHEN ft.TransactionType = 'Deposit' THEN ft.Amount
                ELSE -ft.Amount
            END as BalanceChange
        FROM DimCustomer c
        JOIN DimAccount a ON c.CustomerID = a.CustomerID
        LEFT JOIN FactTransaction ft ON a.AccountID = ft.AccountID
        WHERE c.CustomerName LIKE '%' + UPPER(@name) + '%'
        AND a.Status = 'active'
    )
    SELECT 
        CustomerName,
        AccountType,
        Balance,
        Balance - SUM(ISNULL(BalanceChange, 0)) as CurrentBalance
    FROM CustomerTransactions
    GROUP BY CustomerName, AccountType, Balance
    ORDER BY AccountType;
END;
GO

EXEC BalancePerCustomer @name = 'shelly';

USE DWH;

-- Test 1: Cek struktur tabel dan relationship
SELECT 
    t.name AS TableName,
    c.name AS ColumnName,
    ty.name AS DataType,
    c.max_length AS Length,
    c.is_nullable AS Nullable
FROM sys.tables t
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
WHERE t.name IN ('DimBranch', 'DimAccount', 'DimCustomer', 'FactTransaction')
ORDER BY t.name, c.column_id;

-- Test 2: Cek jumlah data dan data quality
SELECT 
    'DimBranch' AS TableName, 
    COUNT(*) AS RecordCount,
    (SELECT COUNT(*) FROM DimBranch WHERE BranchName IS NULL) AS NullNames
FROM DimBranch

UNION ALL

SELECT 
    'DimAccount', 
    COUNT(*),
    (SELECT COUNT(*) FROM DimAccount WHERE AccountType IS NULL OR Status IS NULL)
FROM DimAccount

UNION ALL

SELECT 
    'DimCustomer', 
    COUNT(*),
    (SELECT COUNT(*) FROM DimCustomer WHERE CustomerName IS NULL OR Email IS NULL)
FROM DimCustomer

UNION ALL

SELECT 
    'FactTransaction', 
    COUNT(*),
    (SELECT COUNT(*) FROM FactTransaction WHERE Amount IS NULL OR TransactionType IS NULL)
FROM FactTransaction;


-- Test 3: Pastikan transformasi data sesuai requirement
-- a. Cek PascalCase column names
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'DimCustomer'
ORDER BY ORDINAL_POSITION;

-- b. Cek uppercase untuk kolom tertentu di DimCustomer
SELECT TOP 5 
    CustomerName, 
    Address, 
    CityName, 
    StateName, 
    Gender,
    Email  -- harus lowercase
FROM DimCustomer;

-- c. Cek tidak ada duplikat di FactTransaction
SELECT TransactionID, COUNT(*) as DuplicateCount
FROM FactTransaction
GROUP BY TransactionID
HAVING COUNT(*) > 1;

-- Test 4a: DailyTransaction dengan berbagai parameter
PRINT '=== TEST DailyTransaction ===';

-- Test case 1: Rentang tanggal normal
PRINT 'Test 1: Rentang tanggal 2024-01-18 sampai 2024-01-22';
EXEC DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-22';

-- Test case 2: Rentang tanggal sempit
PRINT 'Test 2: Rentang tanggal 2024-01-20 saja';
EXEC DailyTransaction @start_date = '2024-01-20', @end_date = '2024-01-20';

-- Test case 3: Tanggal tanpa transaksi
PRINT 'Test 3: Tanggal tanpa transaksi (2024-02-01)';
EXEC DailyTransaction @start_date = '2024-02-01', @end_date = '2024-02-01';

-- Test 5: BalancePerCustomer dengan berbagai nama
PRINT '=== TEST BalancePerCustomer ===';

-- Test case 1: Customer yang ada transaksi
PRINT 'Test 1: Customer SHELLY (harusnya ada data)';
EXEC BalancePerCustomer @name = 'shelly';

-- Test case 2: Customer lain
PRINT 'Test 2: Customer lain (coba nama yang ada di data)';
EXEC BalancePerCustomer @name = 'juwita';

-- Test case 3: Partial name match
PRINT 'Test 3: Partial name match';
EXEC BalancePerCustomer @name = 'shel';

-- Test case 4: Customer tidak ada
PRINT 'Test 4: Customer tidak ada';
EXEC BalancePerCustomer @name = 'nonexistent';


-- Test 6: Foreign key consistency
PRINT '=== TEST DATA INTEGRITY ===';

-- a. Cek FactTransaction references
SELECT 
    'Missing AccountID' AS Issue,
    ft.AccountID
FROM FactTransaction ft
LEFT JOIN DimAccount da ON ft.AccountID = da.AccountID
WHERE da.AccountID IS NULL

UNION ALL

-- b. Cek DimAccount references  
SELECT 
    'Missing CustomerID',
    da.CustomerID
FROM DimAccount da
LEFT JOIN DimCustomer dc ON da.CustomerID = dc.CustomerID
WHERE dc.CustomerID IS NULL;

-- Test 7: Manual verification BalancePerCustomer logic
PRINT '=== MANUAL BALANCE VERIFICATION ===';

-- Untuk customer Shelly, verifikasi perhitungan manual
SELECT 
    c.CustomerName,
    a.AccountType,
    a.Balance AS InitialBalance,
    ft.TransactionType,
    ft.Amount,
    CASE 
        WHEN ft.TransactionType = 'Deposit' THEN ft.Amount
        ELSE -ft.Amount
    END AS BalanceChange
FROM DimCustomer c
JOIN DimAccount a ON c.CustomerID = a.CustomerID
LEFT JOIN FactTransaction ft ON a.AccountID = ft.AccountID
WHERE c.CustomerName LIKE '%SHELLY%'
AND a.Status = 'active'
ORDER BY a.AccountType, ft.TransactionDate;

-- Test 8: Edge cases dan error handling
PRINT '=== EDGE CASE TESTING ===';

-- Test invalid dates
PRINT 'Test: Invalid date range (end before start)';
EXEC DailyTransaction @start_date = '2024-01-22', @end_date = '2024-01-18';

-- Test empty string
PRINT 'Test: Empty customer name';
EXEC BalancePerCustomer @name = '';

-- Test NULL (akan error, tapi cek behavior)
PRINT 'Test: NULL parameter';
-- EXEC BalancePerCustomer @name = NULL; -- Ini akan error, itu expected