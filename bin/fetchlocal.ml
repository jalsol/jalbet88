open Core
open Async
module Duckdb = Jalbet.Duckdb

type matched_data =
  { datetime : string
  ; tickersymbol : string
  ; price : float
  }
[@@deriving sexp_of]

let load_env_file path =
  try
    In_channel.read_lines path
    |> List.filter_map ~f:(fun line ->
      if String.is_empty line || String.is_prefix line ~prefix:"#"
      then None
      else (
        match String.lsplit2 line ~on:'=' with
        | Some (key, value) -> Some (key, value)
        | None -> None))
    |> String.Map.of_alist_exn
  with
  | _ -> String.Map.empty
;;

let get_connection_uri env =
  let get key =
    Map.find env key |> Option.value_exn ~error:(Error.of_string @@ key ^ " not set")
  in
  let[@warning "-8"] [ user; password; host; port; dbname ] =
    List.map ~f:get [ "DB_USER"; "DB_PASSWORD"; "DB_HOST"; "DB_PORT"; "DB_NAME" ]
  in
  sprintf "postgresql://%s:%s@%s:%s/%s" user password host port dbname |> Uri.of_string
;;

let query_sql =
  {|
    SELECT m.datetime, m.tickersymbol, m.price
    FROM "quote"."matched" m
    LEFT JOIN "quote"."total" v
    ON m.tickersymbol = v.tickersymbol
    AND m.datetime = v.datetime
    WHERE m.datetime >= '2025-12-01' AND m.datetime < '2026-01-01'
    AND m.tickersymbol LIKE 'VN30F2%'
    ORDER BY m.datetime
  |}
;;

let matched_data_type =
  let decode (datetime, tickersymbol, price) = Ok { datetime; tickersymbol; price } in
  Caqti_type.(
    custom ~encode:(fun _ -> failwith "unused") ~decode (t3 string string float))
;;

let query = Caqti_request.Infix.(Caqti_type.unit ->* matched_data_type) @@ query_sql

let save_to_duckdb db_path rows =
  In_thread.run
  @@ fun () ->
  Duckdb.with_database db_path ~f:(fun db ->
    Duckdb.with_connection db ~f:(fun conn ->
      printf "Creating table...\n";
      Duckdb.MatchedData.create_table conn;
      printf "Inserting %d rows in batches...\n" @@ List.length rows;
      List.chunks_of rows ~length:1000
      |> List.iteri ~f:(fun i batch ->
        batch
        |> List.map ~f:(fun r -> r.datetime, r.tickersymbol, r.price)
        |> Duckdb.MatchedData.insert_batch conn;
        if (i + 1) % 10 = 0 then printf "Inserted %d batches...\n" (i + 1));
      printf "Successfully saved to DuckDB\n"))
;;

let run_query env db_path () =
  let open Deferred.Let_syntax in
  match%bind get_connection_uri env |> Caqti_async.connect with
  | Error err -> return @@ Error err
  | Ok (module Db : Caqti_async.CONNECTION) ->
    printf "Fetching data from PostgreSQL...\n";
    (match%bind Db.fold query List.cons () [] with
     | Error err -> return @@ Error err
     | Ok results ->
       let results = List.rev results in
       printf "Total number of ticks: %d\n" @@ List.length results;
       printf "The first five ticks are:\n";
       List.take results 5 |> List.iter ~f:(printf !"%{sexp: matched_data}\n");
       save_to_duckdb db_path results >>| fun () -> Ok ())
;;

let () =
  let env = load_env_file ".env" in
  let open Command.Let_syntax in
  Command.async ~summary:"Fetch matched data from PostgreSQL and save to DuckDB"
  @@ [%map_open
       let db_path =
         flag
           "-o"
           (optional_with_default "matched_data.duckdb" string)
           ~doc:"PATH output database path"
       in
       fun () ->
         run_query env db_path ()
         >>= function
         | Ok () -> Deferred.unit
         | Error err ->
           eprintf "Error: %s\n" @@ Caqti_error.show err;
           Deferred.unit]
  |> Command_unix.run
;;
