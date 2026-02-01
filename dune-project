(lang dune 3.20)

(name jalbet)

(generate_opam_files true)

(source
 (github jalsol/jalbet88))

(authors "Nguyen Quang Truong <nqtruong.1002@gmail.com>")

(maintainers "Nguyen Quang Truong <nqtruong.1002@gmail.com>")

(license MIT)

(package
 (name jalbet)
 (synopsis "CS408 bet bot")
 (depends
   ocaml dune
   nx talon hugin
   ctypes ctypes-foreign
   core async
   caqti caqti-async caqti-driver-postgresql))

