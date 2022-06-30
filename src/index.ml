let src = Logs.Src.create "ocaml_ci.index" ~doc:"ocaml-ci indexer"
module Log = (val Logs.src_log src : Logs.LOG)

module Db = Current.Db
module Github = Current_github
module Job_map = Astring.String.Map

type t = {
  db : Sqlite3.db;
  record_job : Sqlite3.stmt;
  remove : Sqlite3.stmt;
  get_job_ids : Sqlite3.stmt;
  full_hash : Sqlite3.stmt;
}

type job_state = [`Not_started | `Active | `Failed of string | `Passed | `Aborted ] [@@deriving show]

type build_status = [ `Not_started | `Pending | `Failed | `Passed ]

let or_fail label x =
  match x with
  | Sqlite3.Rc.OK -> ()
  | err -> Fmt.failwith "Sqlite3 %s error: %s" label (Sqlite3.Rc.to_string err)

let is_valid_hash hash =
  let open Astring in
  String.length hash >= 6 && String.for_all Char.Ascii.is_alphanum hash

let db = lazy (
  let db = Lazy.force Current.Db.v in
  Current_cache.Db.init ();
  Sqlite3.exec db {|
CREATE TABLE IF NOT EXISTS deployer_index (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  owner     TEXT NOT NULL,
  name      TEXT NOT NULL,
  hash      TEXT NOT NULL,
  job_id    TEXT
)|} |> or_fail "create table";
  let record_job = Sqlite3.prepare db "INSERT INTO deployer_index \
                                     (owner, name, hash, job_id) \
                                     VALUES (?, ?, ?, ?)" in
  let remove = Sqlite3.prepare db "DELETE FROM deployer_index \
                                     WHERE owner = ? AND name = ? AND hash = ?" in
  let get_job_ids = Sqlite3.prepare db "SELECT DISTINCT job_id FROM deployer_index \
                                     WHERE owner = ? AND name = ? AND hash = ?" in
  let full_hash = Sqlite3.prepare db "SELECT DISTINCT hash FROM deployer_index \
                                      WHERE owner = ? AND name = ? AND hash LIKE ?" in
      {
        db;
        record_job;
        remove;
        get_job_ids;
        full_hash
      }
)

let init () = ignore (Lazy.force db)

let get_job_ids' t ~owner ~name ~hash =
  Db.query t.get_job_ids Sqlite3.Data.[ TEXT owner; TEXT name; TEXT hash ]
  |> List.map @@ function
  | Sqlite3.Data.[ NULL ] -> None
  | Sqlite3.Data.[ TEXT id ] -> Some id
  | row -> Fmt.failwith "get_job_ids: invalid row %a" Db.dump_row row


module Status_cache = struct
  let cache = Hashtbl.create 1_000
  (* let cache_max_size = 1_000_000 *)

  type elt = [ `Not_started | `Pending | `Failed | `Passed ]

(*   let add ~owner ~name ~hash (status : elt) =
    if Hashtbl.length cache > cache_max_size then Hashtbl.clear cache;
    Hashtbl.add cache (owner, name, hash) status
 *)
  let find ~owner ~name ~hash : elt =
    Hashtbl.find_opt cache (owner, name, hash)
    |> function
      | Some s -> s
      | None -> `Not_started
end

let get_status = Status_cache.find

let record ~repo ~hash (jobs : string option list) =
  let { Github.Repo_id.owner; name } = repo in
  let t = Lazy.force db in
  jobs |> List.map (fun job_id -> 
    Log.info (fun f -> f "@[<h>Index.record %s/%s %s -> %a@]"
                  owner name (Astring.String.with_range ~len:6 hash) Fmt.(option ~none:(any "-") string) job_id);
    match job_id with
    | None -> Db.exec t.record_job Sqlite3.Data.[ TEXT owner; TEXT name; TEXT hash; NULL ]
    | Some id -> Db.exec t.record_job Sqlite3.Data.[ TEXT owner; TEXT name; TEXT hash; TEXT id ]
  ) 
  |> ignore
  (* let () = Status_cache.add ~owner ~name ~hash status in *)
  (* let jobs = Job_map.of_list (List.map (fun job -> (hash, job)) jobs) in
  let previous = 
    get_job_ids' t ~owner ~name ~hash 
    |> List.map (fun x -> (hash, x)) 
    |> Job_map.of_list in
  let merge hash prev job =
    let set job_id =
      Log.info (fun f -> f "@[<h>Index.record %s/%s %s -> %a@]"
                   owner name (Astring.String.with_range ~len:6 hash) Fmt.(option ~none:(any "-") string) job_id);
      match job_id with
      | None -> Db.exec t.record_job Sqlite3.Data.[ TEXT owner; TEXT name; TEXT hash; NULL ]
      | Some id -> Db.exec t.record_job Sqlite3.Data.[ TEXT owner; TEXT name; TEXT hash; TEXT id ]
    in
    let update j1 j2 =
      match j1, j2 with
      | Some j1, Some j2 when j1 = j2 -> ()
      | None, None -> ()
      | _, j2 -> set j2
    in
    let remove () =
      Log.info (fun f -> f "@[<h>Index.record %s/%s %s REMOVED@]"
                   owner name (Astring.String.with_range ~len:6 hash));
      Db.exec t.remove Sqlite3.Data.[ TEXT owner; TEXT name; TEXT hash ]
    in
    begin match prev, job with
      | Some j1, Some j2 -> update j1 j2
      | None, Some j2 -> set j2
      | Some _, None -> remove ()
      | None, None -> assert false
    end;
    None
  in
  let _ : [`Empty] Job_map.t = Job_map.merge merge previous jobs in
  ()
 *)
let get_full_hash ~owner ~name short_hash =
  let t = Lazy.force db in
  if is_valid_hash short_hash then (
    match Db.query t.full_hash Sqlite3.Data.[ TEXT owner; TEXT name; TEXT (short_hash ^ "%") ] with
    | [] -> Error `Unknown
    | [Sqlite3.Data.[ TEXT hash ]] -> Ok hash
    | [_] -> failwith "full_hash: invalid result!"
    | _ :: _ :: _ -> Error `Ambiguous
  ) else Error `Invalid

let get_job_ids ~owner ~name ~hash =
  let t = Lazy.force db in
  get_job_ids' t ~owner ~name ~hash |> List.filter_map (fun x -> x)

module Owner_set = Set.Make(String)

let active_owners = ref Owner_set.empty
let set_active_owners x = active_owners := x
let get_active_owners () = !active_owners

module Owner_map = Map.Make(String)
module Repo_set = Set.Make(String)

let active_repos = ref Owner_map.empty
let set_active_repos ~owner x = active_repos := Owner_map.add owner x !active_repos
let get_active_repos ~owner = Owner_map.find_opt owner !active_repos |> Option.value ~default:Repo_set.empty

module Repo_map = Map.Make(Repo_id)
module Ref_map = Map.Make(String)

let active_refs : string Ref_map.t Repo_map.t ref = ref Repo_map.empty

let set_active_refs ~repo refs =
  active_refs := Repo_map.add repo refs !active_refs

let get_active_refs repo =
  Repo_map.find_opt repo !active_refs |> Option.value ~default:Ref_map.empty
