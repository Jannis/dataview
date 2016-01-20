#!/usr/bin/env boot

(set-env!
 :source-paths #{"src" "test"}
 :resource-paths #{"resources"}
 :dependencies '[;; Boot
                 [adzerk/boot-test "1.0.6"]

                 ;; General
                 [com.datomic/datomic-free "0.9.5344"]
                 [com.rpl/specter "0.9.1"]
                 [datascript "0.15.0"]
                 [datomic-schema "1.3.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]

                 ;; Tests
                 [org.clojure/test.check "0.9.0"]])

(task-options!
 pom {:project 'dataview
      :version "0.1.0-SNAPSHOT"})

(require '[adzerk.boot-test :refer [test]])

(deftask autotest
  []
  (comp (watch)
     (test)))
