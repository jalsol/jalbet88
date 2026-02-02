open Core
open Ctypes
open Foreign

type matched_data =
  { datetime : string
  ; tickersymbol : string
  ; price : float
  }
[@@deriving sexp_of]

(* DuckDB C API types *)
type duckdb_result

let duckdb_result : duckdb_result structure typ = structure "duckdb_result"
let _ = field duckdb_result "internal" (array 48 char)
let () = seal duckdb_result

type state =
  | Success
  | Failure

let state_of_int = function
  | 0 -> Success
  | _ -> Failure
;;

(* Low-level FFI bindings *)
module FFI = struct
  let () =
    ignore (Dl.dlopen ~filename:"libduckdb.so" ~flags:[ Dl.RTLD_NOW; Dl.RTLD_GLOBAL ])
  ;;

  let open_db = foreign "duckdb_open" (string @-> ptr (ptr void) @-> returning int)
  let close_db = foreign "duckdb_close" (ptr (ptr void) @-> returning void)
  let connect = foreign "duckdb_connect" (ptr void @-> ptr (ptr void) @-> returning int)
  let disconnect = foreign "duckdb_disconnect" (ptr (ptr void) @-> returning void)

  let query =
    foreign "duckdb_query" (ptr void @-> string @-> ptr duckdb_result @-> returning int)
  ;;

  let destroy = foreign "duckdb_destroy_result" (ptr duckdb_result @-> returning void)
  let row_count = foreign "duckdb_row_count" (ptr duckdb_result @-> returning int64_t)

  let column_count =
    foreign "duckdb_column_count" (ptr duckdb_result @-> returning int64_t)
  ;;

  let column_name =
    foreign "duckdb_column_name" (ptr duckdb_result @-> int64_t @-> returning string)
  ;;

  let value_varchar =
    foreign
      "duckdb_value_varchar"
      (ptr duckdb_result @-> int64_t @-> int64_t @-> returning string)
  ;;
end

(* Connection management *)
let with_database path ~f =
  let db_ptr = allocate (ptr void) null in
  match state_of_int @@ FFI.open_db path db_ptr with
  | Failure -> failwithf "Failed to open database: %s" path ()
  | Success ->
    Exn.protect ~f:(fun () -> f !@db_ptr) ~finally:(fun () -> FFI.close_db db_ptr)
;;

let with_connection db ~f =
  let conn_ptr = allocate (ptr void) null in
  match state_of_int @@ FFI.connect db conn_ptr with
  | Failure -> failwith "Failed to connect to database"
  | Success ->
    Exn.protect ~f:(fun () -> f !@conn_ptr) ~finally:(fun () -> FFI.disconnect conn_ptr)
;;

(* Query execution *)
let execute conn sql =
  let result = allocate_n duckdb_result ~count:1 in
  match state_of_int @@ FFI.query conn sql result with
  | Failure ->
    FFI.destroy result;
    Error.of_string sql |> Error.raise
  | Success ->
    FFI.destroy result;
    Ok ()
;;

let execute_exn conn sql = execute conn sql |> Result.ok_or_failwith

let query_fold conn sql ~init ~f =
  let result = allocate_n duckdb_result ~count:1 in
  match state_of_int @@ FFI.query conn sql result with
  | Failure ->
    FFI.destroy result;
    Error.of_string sql |> Error.raise
  | Success ->
    let rows = FFI.row_count result |> Int64.to_int_exn in
    let cols = FFI.column_count result |> Int64.to_int_exn in
    let acc =
      List.fold (List.range 0 rows) ~init ~f:(fun acc row_idx ->
        let row =
          List.init cols ~f:(fun col_idx ->
            FFI.value_varchar result (Int64.of_int col_idx) (Int64.of_int row_idx))
        in
        f acc row)
    in
    FFI.destroy result;
    Ok acc
;;

let query_list conn sql =
  query_fold conn sql ~init:[] ~f:(fun acc row -> row :: acc) |> Result.map ~f:List.rev
;;

let query_list_exn conn sql = query_list conn sql |> Result.ok_or_failwith

(* Convenience functions for matched_data table *)
module MatchedData = struct
  let create_table conn =
    execute_exn
      conn
      {|CREATE TABLE IF NOT EXISTS matched_data (
          datetime TIMESTAMP,
          tickersymbol VARCHAR,
          price DOUBLE
        )|}
  ;;

  let insert_batch conn rows =
    rows
    |> List.map ~f:(fun { datetime; tickersymbol; price } ->
      sprintf "('%s','%s',%f)" datetime tickersymbol price)
    |> String.concat ~sep:","
    |> sprintf "INSERT INTO matched_data VALUES %s"
    |> execute_exn conn
  ;;

  let load_all conn =
    query_list_exn conn "SELECT datetime, tickersymbol, price FROM matched_data"
    |> List.map ~f:(function
      | [ datetime; tickersymbol; price ] ->
        { datetime; tickersymbol; price = Float.of_string price }
      | _ -> failwith "Invalid row format")
  ;;

  let count conn =
    match query_list_exn conn "SELECT COUNT(*) FROM matched_data" with
    | [ [ count ] ] -> Int.of_string count
    | _ -> failwith "Failed to get count"
  ;;
end
