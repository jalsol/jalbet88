open Core
open Async

type matched_data =
  { datetime : string
  ; tickersymbol : string
  ; price : float
  }
[@@deriving sexp_of]

let get_connection_uri () =
  let[@warning "-8"] [ user; password; host; port; dbname ] =
    List.map
      ~f:(fun var ->
        var |> Sys.getenv |> Option.value_exn ~error:(Error.of_string @@ var ^ " not set"))
      [ "DB_USER"; "DB_PASSWORD"; "DB_HOST"; "DB_PORT"; "DB_NAME" ]
  in
  Uri.of_string @@ sprintf "postgresql://%s:%s@%s:%s/%s" user password host port dbname
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
    custom ~encode:(fun _ -> failwith "encode unused") ~decode (t3 string string float))
;;

let query = Caqti_request.Infix.(Caqti_type.unit ->* matched_data_type) @@ query_sql

let run_query () =
  let open Deferred.Result.Let_syntax in
  let%bind (module Db : Caqti_async.CONNECTION) =
    Caqti_async.connect @@ get_connection_uri ()
  in
  let%bind results = Db.fold query List.cons () [] in
  let results = List.rev results in
  printf "Total number of ticks: %d\n" @@ List.length results;
  printf "The first five ticks are:\n";
  List.take results 5 |> List.iter ~f:(printf !"%{sexp: matched_data}\n");
  return ()
;;

let () =
  Command.async
    ~summary:"Fetch matched data from PostgreSQL"
    Command.Param.(
      return (fun () ->
        run_query ()
        >>= function
        | Ok () -> Deferred.unit
        | Error err ->
          eprintf "Error: %s\n" @@ Caqti_error.show err;
          Deferred.unit))
  |> Command_unix.run
;;
