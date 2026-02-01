open Core
module Duckdb = Jalbet.Duckdb

(* Setup test database with sample data *)
let setup_test_db () =
  let db_path = "test_data.duckdb" in
  (* Remove old test db if exists *)
  (try Core_unix.unlink db_path with
   | _ -> ());
  (* Create and populate test database *)
  Duckdb.with_database db_path ~f:(fun db ->
    Duckdb.with_connection db ~f:(fun conn ->
      Duckdb.MatchedData.create_table conn;
      let test_data =
        [ "2025-01-01 10:00:00", "TEST01", 100.0
        ; "2025-01-01 10:01:00", "TEST01", 101.5
        ; "2025-01-01 10:02:00", "TEST01", 99.8
        ; "2025-01-01 10:03:00", "TEST02", 200.0
        ; "2025-01-01 10:04:00", "TEST02", 205.5
        ]
      in
      Duckdb.MatchedData.insert_batch conn test_data));
  db_path
;;

let%expect_test "count rows in test database" =
  let db_path = setup_test_db () in
  Duckdb.with_database db_path ~f:(fun db ->
    Duckdb.with_connection db ~f:(fun conn ->
      let count = Duckdb.MatchedData.count conn in
      printf "Row count: %d\n" count;
      [%expect {| Row count: 5 |}]))
;;

let%expect_test "load all rows from test database" =
  let db_path = setup_test_db () in
  Duckdb.with_database db_path ~f:(fun db ->
    Duckdb.with_connection db ~f:(fun conn ->
      let all_data = Duckdb.MatchedData.load_all conn in
      List.iter all_data ~f:(fun (dt, sym, price) ->
        let date_time = String.prefix dt 16 in
        printf "%s | %s | %.1f\n" date_time sym price);
      [%expect
        {|
        2025-01-01 10:00 | TEST01 | 100.0
        2025-01-01 10:01 | TEST01 | 101.5
        2025-01-01 10:02 | TEST01 | 99.8
        2025-01-01 10:03 | TEST02 | 200.0
        2025-01-01 10:04 | TEST02 | 205.5
        |}]))
;;

let%expect_test "query specific ticker" =
  let db_path = setup_test_db () in
  Duckdb.with_database db_path ~f:(fun db ->
    Duckdb.with_connection db ~f:(fun conn ->
      let result =
        Duckdb.query_list_exn
          conn
          "SELECT tickersymbol, price FROM matched_data WHERE tickersymbol = 'TEST01'"
      in
      List.iter result ~f:(fun row ->
        match row with
        | [ sym; price ] -> printf "%s: %s\n" sym price
        | _ -> printf "Invalid row\n");
      [%expect
        {|
        TEST01: 100.0
        TEST01: 101.5
        TEST01: 99.8
        |}]))
;;

let%expect_test "aggregate queries" =
  let db_path = setup_test_db () in
  Duckdb.with_database db_path ~f:(fun db ->
    Duckdb.with_connection db ~f:(fun conn ->
      let stats =
        Duckdb.query_list_exn
          conn
          "SELECT MIN(price), MAX(price), AVG(price) FROM matched_data"
      in
      (match stats with
       | [ [ min_p; max_p; avg_p ] ] ->
         let min_f = Float.of_string min_p in
         let max_f = Float.of_string max_p in
         let avg_f = Float.of_string avg_p in
         printf "Min: %.1f, Max: %.1f, Avg: %.2f\n" min_f max_f avg_f
       | _ -> printf "Failed to get stats\n");
      [%expect {| Min: 99.8, Max: 205.5, Avg: 141.36 |}]))
;;

let%expect_test "group by query" =
  let db_path = setup_test_db () in
  Duckdb.with_database db_path ~f:(fun db ->
    Duckdb.with_connection db ~f:(fun conn ->
      let by_ticker =
        Duckdb.query_list_exn
          conn
          "SELECT tickersymbol, COUNT(*) as cnt FROM matched_data GROUP BY tickersymbol \
           ORDER BY cnt DESC"
      in
      List.iter by_ticker ~f:(fun row ->
        match row with
        | [ sym; cnt ] -> printf "%s: %s\n" sym cnt
        | _ -> printf "Invalid row\n");
      [%expect
        {|
        TEST01: 3
        TEST02: 2
        |}]))
;;
