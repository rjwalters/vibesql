//! Test for Issue #2599: IN subquery returns incorrect results
//!
//! This test reproduces the bug where IN subqueries in OR expressions
//! fail to return expected rows due to cross-type comparison issues
//! in hash-based semi-join.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor, IndexExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Helper to execute SQL and return results
fn execute_sql(db: &mut Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).expect("Parse error");
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).expect("Execution error")
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db).expect("Create table error");
            vec![]
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt).expect("Insert error");
            vec![]
        }
        vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
            IndexExecutor::execute(&create_index_stmt, db).expect("Create index error");
            vec![]
        }
        _ => panic!("Unexpected statement type"),
    }
}

#[test]
fn test_issue_2599_simple_in_subquery() {
    let mut db = Database::new();

    // Create a simple table
    execute_sql(
        &mut db,
        "CREATE TABLE tab1 (pk INTEGER PRIMARY KEY, col0 INTEGER, col3 INTEGER)",
    );

    // Insert test data (simplified from sqllogictest)
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (1, 10, 100)");
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (2, 20, 200)");
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (3, 100, 10)"); // col3=10 matches col0=10
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (4, 200, 20)"); // col3=20 matches col0=20

    // Simple IN subquery: col3 IN (SELECT col0 FROM tab1)
    // Should match rows where col3 is in {10, 20, 100, 200}
    let result = execute_sql(
        &mut db,
        "SELECT pk FROM tab1 WHERE col3 IN (SELECT col0 FROM tab1) ORDER BY pk",
    );

    println!("Simple IN result: {:?}", result);
    assert_eq!(
        result.len(),
        4,
        "All rows should match - col3 values 100,200,10,20 are all in col0"
    );
}

#[test]
fn test_issue_2599_in_subquery_with_or() {
    let mut db = Database::new();

    // Create table matching the failing test schema
    execute_sql(
        &mut db,
        "CREATE TABLE tab1 (pk INTEGER PRIMARY KEY, col0 INTEGER, col3 INTEGER)",
    );

    // Insert a few test rows
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (1, 10, 100)");
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (2, 20, 200)");
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (3, 30, 300)");
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (4, 40, 30)"); // col3=30 matches col0=30
    execute_sql(&mut db, "INSERT INTO tab1 VALUES (5, 50, 76)"); // col3=76 (for OR test)

    // IN subquery in OR expression
    // (col3 IN (SELECT col0 WHERE col3 > 100) AND col3 > 25) OR col3 = 76
    let result = execute_sql(
        &mut db,
        "
        SELECT pk FROM tab1
        WHERE (col3 IN (SELECT col0 FROM tab1 WHERE col3 > 100) AND col3 > 25)
           OR col3 = 76
        ORDER BY pk
    ",
    );

    println!("IN with OR result: {:?}", result);

    // col0 values where col3 > 100: {20, 30} (rows 2,3 have col3=200,300)
    // Rows where col3 IN {20,30} AND col3 > 25: row 4 (col3=30)
    // Rows where col3 = 76: row 5
    // Expected: rows 4 and 5

    // Show debug info
    let subquery = execute_sql(&mut db, "SELECT col0 FROM tab1 WHERE col3 > 100");
    println!(
        "Subquery result (col0 WHERE col3 > 100): {:?}",
        subquery
    );

    let all_rows = execute_sql(&mut db, "SELECT pk, col0, col3 FROM tab1");
    println!("All rows: {:?}", all_rows);

    assert_eq!(result.len(), 2, "Expected rows 4 and 5");
}

#[test]
fn test_issue_2599_complex_or_with_index() {
    // This test attempts to reproduce the exact failing scenario from sqllogictest
    // with an index on col0 to trigger the index optimization path
    let mut db = Database::new();

    // Create table matching the failing test schema
    execute_sql(
        &mut db,
        "CREATE TABLE tab0 (pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
    );

    // Create index on col0 to trigger index optimization
    execute_sql(&mut db, "CREATE INDEX idx_col0 ON tab0 (col0)");

    // Insert test data similar to sqllogictest
    // Row 71: pk=71, col0=832, col1=381.52, col3=1, col4=916.28
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (71, 832, 381.52, 'test', 1, 916.28, 'test')");
    // Row 90: pk=90, col0=434, col1=776.32, col3=665, col4=903.75
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (90, 434, 776.32, 'test', 665, 903.75, 'test')");
    // Row 77: pk=77, col0=665 (this is the key: col3=665 in row 90 should match col0=665 in row 77)
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (77, 665, 18.68, 'test', 781, 90.24, 'test')");
    // Row 26: pk=26, col0=385, col1=281.24, col3=125
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (26, 385, 281.24, 'test', 125, 367.33, 'test')");

    // First, let's verify the subquery returns the right values
    // col3 >= 605 should include row 77 (col3=781) and row 90 (col3=665)
    let subquery_result = execute_sql(&mut db, "SELECT col0 FROM tab0 WHERE col3 >= 605");
    println!("Subquery result (col0 WHERE col3 >= 605): {:?}", subquery_result);
    // Should include col0=665 (from row 77) and col0=434 (from row 90)

    // Simplified version of the failing query
    // The key pattern: (col3 IN (subquery) AND col3 > 89) OR col3 = 76
    let result = execute_sql(
        &mut db,
        "SELECT pk FROM tab0 WHERE (col3 IN (SELECT col0 FROM tab0 WHERE col3 >= 605) AND col3 > 89) OR col3 = 76 ORDER BY pk",
    );

    println!("Query result: {:?}", result);

    // col0 values where col3 >= 605: {665, 434} (rows 77 and 90 have col3=781, 665)
    // Row 90 has col3=665. Is 665 in {665, 434}? YES! And 665 > 89? YES!
    // So row 90 should match.

    // Let's also print all rows for debugging
    let all_rows = execute_sql(&mut db, "SELECT pk, col0, col3 FROM tab0 ORDER BY pk");
    println!("All rows: {:?}", all_rows);

    // Check that row 90 is in the result
    let pks: Vec<i64> = result.iter().filter_map(|r| {
        if let vibesql_types::SqlValue::Integer(pk) = &r.values[0] {
            Some(*pk)
        } else {
            None
        }
    }).collect();

    println!("Returned PKs: {:?}", pks);
    assert!(pks.contains(&90), "Row 90 should be in the result because col3=665 is in subquery (col0=665 from row 77)");
}

#[test]
fn test_issue_2599_minimal_reproduction() {
    // Minimal reproduction of issue #2599
    // This test systematically checks each step of the IN subquery evaluation

    let mut db = Database::new();

    // Create table
    execute_sql(
        &mut db,
        "CREATE TABLE tab0 (pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
    );

    // Insert minimal data needed to reproduce
    // Row 3: col0=1, col3=357 (satisfies col3 > 273, so col0=1 goes into subquery)
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (3, 1, 100.50, 'a', 357, 721.9, 'b')");
    // Row 71: col0=832, col3=1 (we check if col3=1 matches subquery containing col0=1)
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (71, 832, 381.52, 'c', 1, 916.28, 'd')");
    // Row 77: col0=665, col3=781 (satisfies col3 >= 605, so col0=665 goes into subquery)
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (77, 665, 18.68, 'e', 781, 90.24, 'f')");
    // Row 90: col0=434, col3=665 (we check if col3=665 matches subquery containing col0=665)
    execute_sql(&mut db, "INSERT INTO tab0 VALUES (90, 434, 776.32, 'g', 665, 903.75, 'h')");

    println!("=== All rows ===");
    let all = execute_sql(&mut db, "SELECT pk, col0, col1, col3 FROM tab0 ORDER BY pk");
    for row in &all {
        println!("  {:?}", row);
    }

    println!("\n=== Step 1: Subquery result (col0 values where col3 >= 605 OR col3 > 273) ===");
    let subquery_result = execute_sql(&mut db, "SELECT col0 FROM tab0 WHERE col3 >= 605 OR col3 > 273 ORDER BY col0");
    let subquery_values: Vec<i64> = subquery_result.iter().filter_map(|r| {
        if let vibesql_types::SqlValue::Integer(v) = &r.values[0] { Some(*v) } else { None }
    }).collect();
    println!("  Subquery returns col0 values: {:?}", subquery_values);
    // Expected: [1, 434, 665] (from rows 3, 90, 77 respectively)

    println!("\n=== Step 2: Check if col3 values are IN the subquery ===");
    // Check col3=1 IN subquery (row 71's col3)
    let in_check_1 = execute_sql(&mut db, "SELECT 1 IN (SELECT col0 FROM tab0 WHERE col3 >= 605 OR col3 > 273)");
    println!("  col3=1 IN subquery: {:?}", in_check_1);

    // Check col3=665 IN subquery (row 90's col3)
    let in_check_665 = execute_sql(&mut db, "SELECT 665 IN (SELECT col0 FROM tab0 WHERE col3 >= 605 OR col3 > 273)");
    println!("  col3=665 IN subquery: {:?}", in_check_665);

    println!("\n=== Step 3: Check col3 > 89 ===");
    let gt_check_1 = execute_sql(&mut db, "SELECT 1 > 89");
    println!("  1 > 89: {:?}", gt_check_1);
    let gt_check_665 = execute_sql(&mut db, "SELECT 665 > 89");
    println!("  665 > 89: {:?}", gt_check_665);

    println!("\n=== Step 4: Combined (col3 IN subquery AND col3 > 89) ===");
    // For row 71: col3=1, expect FALSE (1 IN subquery=TRUE but 1>89=FALSE)
    // For row 90: col3=665, expect TRUE (665 IN subquery=TRUE and 665>89=TRUE)

    println!("\n=== Step 5: Full query ===");
    let result = execute_sql(
        &mut db,
        "SELECT pk FROM tab0 WHERE ((col3 IN (SELECT col0 FROM tab0 WHERE col3 >= 605 OR col3 > 273) AND col3 > 89) OR col3 = 76) AND col1 > 194.50 ORDER BY pk",
    );
    let pks: Vec<i64> = result.iter().filter_map(|r| {
        if let vibesql_types::SqlValue::Integer(pk) = &r.values[0] { Some(*pk) } else { None }
    }).collect();
    println!("  Query result PKs: {:?}", pks);

    // Expected: [90] only
    // - Row 71: col3=1 IN subquery (TRUE) AND col3=1 > 89 (FALSE) = FALSE, col3=76 (FALSE) => FALSE
    // - Row 90: col3=665 IN subquery (TRUE) AND col3=665 > 89 (TRUE) = TRUE => TRUE

    // Assert the key check: row 90 should be in result
    assert!(pks.contains(&90), "Row 90 should be in result: col3=665 is IN subquery AND col3=665 > 89");

    // Row 71 should NOT be in result (col3=1 > 89 is FALSE)
    // But if it IS in the result, that would be a different bug
}

/// This test reproduces the ACTUAL failing scenario from the sqllogictest
/// with 100 rows and indexes created BEFORE data insertion.
#[test]
fn test_issue_2599_full_100rows_with_index() {
    let mut db = Database::new();

    // Create tab0 (without indexes)
    execute_sql(
        &mut db,
        "CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
    );

    // Insert all 100 rows
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(0,47,684.10,'qyjvm',822,427.45,'nglpa')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(1,828,686.72,'wtcqm',469,248.60,'ngwak')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(2,876,836.56,'zzgxt',364,747.1,'hrjvl')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(3,1,100.50,'zqsen',357,721.9,'hbghq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(4,289,642.84,'pjigt',915,231.43,'lvylv')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(5,708,946.89,'oeblm',686,717.48,'afaxi')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(6,755,947.64,'nycos',155,137.16,'xlhke')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(7,58,148.36,'kdzep',260,75.50,'svbla')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(8,22,756.22,'iktns',22,885.22,'wbcwl')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(9,440,788.93,'vdabx',164,789.41,'vwxob')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(10,124,220.34,'xfcyt',743,39.13,'axtyn')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(11,526,96.12,'johwj',764,8.2,'fauvt')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(12,369,660.32,'qrrot',823,337.95,'kcgvd')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(13,266,472.2,'cmpkv',711,41.65,'drnak')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(14,351,546.98,'eupeo',50,659.29,'nmhqz')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(15,897,463.23,'lxmnc',926,350.16,'hdqta')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(16,946,505.70,'tjqrw',497,46.27,'erfrq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(17,934,774.87,'hmabv',627,555.95,'skivt')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(18,308,323.38,'waiwi',513,699.54,'rdbeo')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(19,168,266.62,'wtawp',305,435.94,'xhkzl')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(20,877,931.44,'ndssm',729,968.30,'sbkje')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(21,682,958.68,'vxyxx',727,910.59,'tjiaa')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(22,363,434.85,'avpdt',61,366.26,'jjqqu')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(23,833,23.76,'gbwde',952,81.87,'obhud')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(24,775,81.11,'dbkcn',700,558.68,'dxsio')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(25,260,794.14,'qanmr',312,188.31,'zmado')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(26,385,281.24,'dpgoa',125,367.33,'ydeuq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(27,884,750.59,'ukjaq',428,630.95,'eoazt')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(28,760,599.79,'mwvpl',92,772.54,'clkla')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(29,909,544.44,'bqmtf',194,967.37,'pajgf')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(30,742,764.85,'gfngv',638,904.15,'gwhsq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(31,818,967.55,'cvntj',788,908.69,'gjwwj')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(32,455,623.4,'urlpe',84,62.53,'pjkdi')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(33,375,979.30,'ayyeb',675,326.54,'jwxwl')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(34,305,242.50,'ydhui',30,684.59,'qobdu')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(35,626,692.60,'iusdc',589,246.11,'toogq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(36,978,799.93,'wejyd',365,382.77,'hxcir')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(37,901,368.90,'zuotg',562,849.8,'mhobb')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(38,442,641.53,'ffuhg',235,912.3,'yqcfo')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(39,803,436.17,'fiwhe',888,473.71,'dshjs')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(40,947,271.5,'yldai',101,193.89,'huhkw')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(41,229,478.94,'ulpzk',161,613.75,'qfvil')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(42,23,343.76,'ovvku',683,293.38,'mcnpz')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(43,345,225.22,'vakal',242,919.50,'jywae')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(44,480,676.46,'wevrc',989,444.5,'dfjjd')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(45,606,575.61,'wrqup',281,199.79,'cyssn')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(46,417,240.50,'cytep',307,30.0,'ekyln')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(47,744,572.20,'wwgod',712,415.99,'zwloe')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(48,199,163.73,'xmxfm',205,593.86,'yweng')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(49,777,427.63,'etxri',230,687.99,'ohsup')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(50,489,725.57,'hxhfx',537,831.78,'hnmlg')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(51,997,497.5,'njwoe',840,294.20,'wmvsi')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(52,221,369.94,'ucmnw',432,596.46,'jacwk')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(53,982,300.17,'blbqr',811,661.59,'gpupq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(54,615,693.31,'fruqi',306,899.11,'swtsp')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(55,594,514.22,'jodiu',697,378.63,'olfut')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(56,167,442.66,'wccze',851,906.49,'kjzul')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(57,970,966.95,'lbhdg',333,195.35,'xslrx')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(58,662,533.53,'kpabe',43,941.88,'umwzp')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(59,532,647.65,'qzmov',835,1.30,'zvjpd')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(60,154,20.96,'xeukx',141,481.8,'sbefo')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(61,983,694.15,'thgdc',601,866.54,'taqaj')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(62,993,135.55,'vpioa',685,161.46,'jtkkt')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(63,306,403.80,'yrvmp',311,997.70,'pfnkn')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(64,802,249.42,'dldey',488,773.88,'jxpon')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(65,766,239.48,'wncet',927,764.92,'lhxgk')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(66,490,896.20,'cbluc',659,262.74,'trnqb')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(67,906,809.60,'mdqvx',682,692.86,'lsjap')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(68,232,769.9,'lodct',571,736.0,'zaoso')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(69,559,881.12,'fmodh',105,463.74,'gweiv')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(70,505,712.80,'fwsud',897,452.11,'wsuva')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(71,832,381.52,'xjuvc',1,916.28,'ywftm')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(72,59,723.57,'asjoh',574,828.9,'qfxcl')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(73,111,386.79,'sktpi',454,45.76,'yqzwa')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(74,922,968.17,'smyfg',829,436.46,'zviqq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(75,71,136.49,'iysfy',831,2.97,'bvsiu')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(76,405,767.92,'rmxly',174,5.28,'xqbqk')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(77,665,18.68,'tdklv',781,90.24,'qxhvy')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(78,103,558.64,'qtkcf',392,417.16,'ymthk')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(79,125,585.96,'lwieu',224,776.44,'hcecq')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(80,890,6.71,'scgbs',556,719.21,'yavtg')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(81,964,545.48,'gxqgm',382,678.95,'dmpvx')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(82,921,880.2,'mcxdd',14,102.10,'eqfjj')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(83,981,770.88,'krcnl',737,572.70,'pwivr')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(84,631,118.1,'welgz',401,925.1,'hopje')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(85,252,241.39,'pmeyg',68,388.14,'ptwwt')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(86,741,821.64,'hlsjt',628,696.67,'ltgcy')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(87,137,160.70,'wjykf',430,221.26,'lqjuz')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(88,248,245.40,'nwcls',891,82.32,'czfxy')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(89,275,858.10,'sedzs',150,302.58,'vopmy')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(90,434,776.32,'qdrlf',665,903.75,'dubdf')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(91,591,817.37,'anfpy',853,47.14,'unavg')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(92,860,3.95,'iufkj',714,140.27,'xnbgv')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(93,276,680.83,'atgjz',433,998.52,'yfusi')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(94,370,638.39,'occkg',62,929.73,'cgeqr')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(95,824,246.32,'mcyix',69,880.83,'ddpeo')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(96,88,559.24,'rdjnw',33,560.49,'vwzvo')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(97,1000,468.25,'lmqpp',221,557.72,'toxdg')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(98,418,798.65,'nttxb',236,478.48,'qashh')");
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(99,844,458.52,'kqlhx',636,57.35,'lntro')");

    // Create tab1 with indexes BEFORE inserting data (critical for triggering bug)
    execute_sql(
        &mut db,
        "CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
    );

    // Create indexes BEFORE inserting data
    execute_sql(&mut db, "CREATE INDEX idx_tab1_0 on tab1 (col0)");
    execute_sql(&mut db, "CREATE INDEX idx_tab1_1 on tab1 (col1)");
    execute_sql(&mut db, "CREATE INDEX idx_tab1_3 on tab1 (col3)");
    execute_sql(&mut db, "CREATE INDEX idx_tab1_4 on tab1 (col4)");

    // Copy data from tab0 to tab1
    execute_sql(&mut db, "INSERT INTO tab1 SELECT * FROM tab0");

    // First test on tab0 (no indexes) - should pass
    let result_tab0 = execute_sql(
        &mut db,
        "SELECT pk FROM tab0 WHERE (col3 IN (SELECT col0 FROM tab0 WHERE col3 >= 605 OR (((col0 >= 64) OR (col4 IN (454.84,354.12,41.93,180.48)) OR col3 > 273 OR (((col1 > 334.28 AND (col1 < 615.55 OR (((col4 IS NULL))) OR (col0 > 101) AND col0 IN (384,766,604,640,327) AND col3 IS NULL AND col0 < 384 AND ((((col0 >= 622 AND col0 < 894 OR (col1 > 899.10))))))) AND col1 < 994.43 AND (col0 > 912 AND (col3 <= 992)) AND ((col0 > 759 AND col1 > 738.84 AND col3 IN (992,363,791,703))) OR col0 <= 404 AND col1 >= 792.80 AND col4 >= 133.79 OR col3 >= 565 AND ((col4 <= 340.72)) AND (col3 < 389) AND (((col4 IS NULL AND col0 > 176))) OR (col3 > 703)) OR col1 IS NULL)) AND col3 > 89) OR col3 = 76)) AND col1 > 194.50 ORDER BY pk",
    );
    let pks_tab0: Vec<i64> = result_tab0.iter().filter_map(|r| {
        if let vibesql_types::SqlValue::Integer(pk) = &r.values[0] { Some(*pk) } else { None }
    }).collect();
    println!("tab0 result (no indexes): {:?}", pks_tab0);

    // Test on tab1 (with indexes) - this is the failing case
    let result_tab1 = execute_sql(
        &mut db,
        "SELECT pk FROM tab1 WHERE (col3 IN (SELECT col0 FROM tab1 WHERE col3 >= 605 OR (((col0 >= 64) OR (col4 IN (454.84,354.12,41.93,180.48)) OR col3 > 273 OR (((col1 > 334.28 AND (col1 < 615.55 OR (((col4 IS NULL))) OR (col0 > 101) AND col0 IN (384,766,604,640,327) AND col3 IS NULL AND col0 < 384 AND ((((col0 >= 622 AND col0 < 894 OR (col1 > 899.10))))))) AND col1 < 994.43 AND (col0 > 912 AND (col3 <= 992)) AND ((col0 > 759 AND col1 > 738.84 AND col3 IN (992,363,791,703))) OR col0 <= 404 AND col1 >= 792.80 AND col4 >= 133.79 OR col3 >= 565 AND ((col4 <= 340.72)) AND (col3 < 389) AND (((col4 IS NULL AND col0 > 176))) OR (col3 > 703)) OR col1 IS NULL)) AND col3 > 89) OR col3 = 76)) AND col1 > 194.50 ORDER BY pk",
    );
    let pks_tab1: Vec<i64> = result_tab1.iter().filter_map(|r| {
        if let vibesql_types::SqlValue::Integer(pk) = &r.values[0] { Some(*pk) } else { None }
    }).collect();
    println!("tab1 result (with indexes): {:?}", pks_tab1);

    // Expected: [26, 54, 67, 70, 71, 90, 97]
    let expected = vec![26i64, 54, 67, 70, 71, 90, 97];

    assert_eq!(pks_tab0, expected, "tab0 (no indexes) should return correct results");
    assert_eq!(pks_tab1, expected, "tab1 (with indexes) should return same results as tab0");
}
