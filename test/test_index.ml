module Index = Deployer.Index
module Ref_map = Index.Ref_map

let jobs =
  let state f (variant, state) = Fmt.pf f "%s:%a" variant Index.pp_job_state state in
  Alcotest.testable (Fmt.Dump.list state) (=)

let test_simple () =
  let owner = "owner" in
  let name = "name" in
  let repo = { Current_github.Repo_id.owner; name } in
  let hash = "abc" in
  let _db = Lazy.force Current.Db.v in
  Index.init ();
  Index.record ~repo ~hash [ Some "job1"; None ];
  Alcotest.(check (list string)) "Job-ids" ["job1"] @@ Index.get_job_ids ~owner ~name ~hash;
  Index.record ~repo ~hash [ Some "job2" ];
  Alcotest.(check (list string)) "Job-ids" ["job1"; "job2"] @@ List.sort String.compare @@ Index.get_job_ids ~owner ~name ~hash

let tests = [
    Alcotest_lwt.test_case_sync "simple" `Quick test_simple;
]

