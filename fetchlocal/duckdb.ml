open Core
open Ctypes
open Foreign

(* DuckDB C API types *)
type duckdb_result

let duckdb_result : duckdb_result structure typ = structure "duckdb_result"
let _ = field duckdb_result "internal" (array 48 char)
let () = seal duckdb_result

type state =
  | Success
  | Error

let state_of_int = function
  | 0 -> Success
  | _ -> Error
;;

(* FFI bindings *)
let open_db = foreign "duckdb_open" (string @-> ptr (ptr void) @-> returning int)
let close_db = foreign "duckdb_close" (ptr (ptr void) @-> returning void)
let connect = foreign "duckdb_connect" (ptr void @-> ptr (ptr void) @-> returning int)
let disconnect = foreign "duckdb_disconnect" (ptr (ptr void) @-> returning void)

let query =
  foreign "duckdb_query" (ptr void @-> string @-> ptr duckdb_result @-> returning int)
;;

let destroy = foreign "duckdb_destroy_result" (ptr duckdb_result @-> returning void)

(* Helper functions *)
let with_database path ~f =
  let db_ptr = allocate (ptr void) null in
  match state_of_int @@ open_db path db_ptr with
  | Error -> failwithf "Failed to open database: %s" path ()
  | Success -> Exn.protect ~f:(fun () -> f !@db_ptr) ~finally:(fun () -> close_db db_ptr)
;;

let with_connection db ~f =
  let conn_ptr = allocate (ptr void) null in
  match state_of_int @@ connect db conn_ptr with
  | Error -> failwith "Failed to connect to database"
  | Success ->
    Exn.protect ~f:(fun () -> f !@conn_ptr) ~finally:(fun () -> disconnect conn_ptr)
;;

let execute conn sql =
  let result = allocate_n duckdb_result ~count:1 in
  match state_of_int @@ query conn sql result with
  | Error ->
    destroy result;
    failwithf "Query failed: %s" sql ()
  | Success ->
    destroy result;
    Ok ()
;;

let create_table conn =
  execute conn
    {|CREATE TABLE IF NOT EXISTS matched_data (
        datetime TIMESTAMP,
        tickersymbol VARCHAR,
        price DOUBLE
      )|}
;;

let insert_batch conn rows =
  rows
  |> List.map ~f:(fun (dt, sym, price) -> sprintf "('%s','%s',%f)" dt sym price)
  |> String.concat ~sep:","
  |> sprintf "INSERT INTO matched_data VALUES %s"
  |> execute conn
;;
