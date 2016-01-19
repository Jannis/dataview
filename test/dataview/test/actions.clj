(ns dataview.test.actions
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.rpl.specter :refer :all]
            [datomic.api :as d]
            [datomic-schema.schema :as ds]
            [dataview.core :as dv]
            [dataview.test.setup :as setup :refer [create-db
                                                   delete-db
                                                   with-conn]]))

(defn create-schemas [conn schemas]
  (println "create-schemas")
  @(d/transact conn (concat
                     (ds/generate-parts [(ds/part "test")])
                     (ds/generate-schema schemas))))

(defn define-data-views []
  (def Action
    (dv/dataview
     {:name   :action
      :schema {:name [:string :indexed]
               :amount [:long]}
      :data   '[:find [(pull ?a QUERY) ...]
                :in $ QUERY
                :where [?a :action/name _]]
      :derived-attrs
      {:pledges {:data '[:find (count ?p) .
                         :in $ ?a
                         :where [?p :pledge/action ?a]
                                [?u :user/pledges ?p]
                                [?u :user/verified? true]]
                 :transform [[ALL] #(or % 0)]}}}))

  (def Pledge
    (dv/dataview
     {:name   :pledge
      :schema {:moment [:long]
               :action {[:ref] Action}}
      :data   '[:find [(pull ?p QUERY) ...]
                :in $ QUERY
                :where [?p :pledge/action _]]}))

  (def User
    (dv/dataview
     {:name   :user
      :schema {:email [:string :indexed]
               :pledges {[:ref :many] Pledge}
               :verified? [:boolean]}
      :data   '[:find [(pull ?u QUERY) ...]
                :in $ QUERY
                :where [?u :user/email _]]})))

(defn define-container []
  (with-conn
    (def Container
      (dv/container {:views #{Action Pledge User}
                     :conn conn
                     :create-schemas #(create-schemas conn %)
                     :notify #()}))))

(defn setup []
  (delete-db)
  (create-db)
  (define-data-views)
  (define-container))

(defn teardown []
  (delete-db))

(defn data-views-fixture [f]
  (setup)
  (f)
  (teardown))

(use-fixtures :each data-views-fixture)

(deftest actions
  (println "USERS IN EMPTY DB")
  (pprint (dv/query User '[*]))

  (with-conn
    @(d/transact conn [{:db/id (d/tempid :db.part/test -1)
                        :user/email "jeff@jeff.org"
                        :user/verified? false}
                       {:db/id (d/tempid :db.part/test -2)
                        :user/email "linda@linda.org"
                        :user/verified? false}
                       {:db/id (d/tempid :db.part/test -3)
                        :user/email "joe@joe.org"
                        :user/verified? false}
                       {:db/id (d/tempid :db.part/test -4)
                        :action/name "One"
                        :action/amount 1}
                       {:db/id (d/tempid :db.part/test -5)
                        :action/name "Two"
                        :action/amount 2}
                       {:db/id (d/tempid :db.part/test -6)
                        :pledge/moment 100
                        :pledge/action
                        (d/tempid :db.part/test -4)}
                       {:db/id (d/tempid :db.part/test -7)
                        :pledge/moment 200
                        :pledge/action
                        (d/tempid :db.part/test -4)}
                       {:db/id (d/tempid :db.part/test -8)
                        :pledge/moment 300
                        :pledge/action
                        (d/tempid :db.part/test -5)}]))

  (Thread/sleep 1000)

  (println "INITIAL USERS")
  (pprint (dv/query User '[*]))

  (println "INITIAL PLEDGES")
  (pprint (dv/query Pledge '[*]))

  (println "INITIAL ACTIONS")
  (pprint (dv/query Action '[*]))

  (with-conn
    (let [joe     (first (dv/query User '[*]
                                   [['?x :user/email "joe@joe.org"]]))
          linda   (first (dv/query User '[*]
                                   [['?x :user/email "linda@linda.org"]]))
          jeff    (first (dv/query User '[*]
                                   [['?x :user/email "jeff@jeff.org"]]))
          pledges (dv/query Pledge '[*])]
      @(d/transact conn
                   [(merge joe   {:user/pledges (pledges 0)})
                    (merge linda {:user/pledges (pledges 1)})
                    (merge jeff  {:user/pledges (pledges 2)})])))

  (Thread/sleep 1000)

  (println "USERS AFTER PLEDING")
  (pprint (dv/query User '[*]))

  (println "PLEDGES AFTER PLEDGING")
  (pprint (dv/query Pledge '[*]))

  (println "ACTIONS AFTER PLEDGING")
  (println "Note: :action/pledges all 0 because none of the")
  (println "      users are verified yet")
  (pprint (dv/query Action '[*]))

  (with-conn
    (let [users (dv/query User '[*])]
      @(d/transact conn (mapv #(assoc % :user/verified? true) users))))

  (Thread/sleep 1000)

  (println "USERS AFTER VERIFICATION")
  (pprint (dv/query User '[*]))

  (println "PLEDGES AFTER VERIFICATION")
  (pprint (dv/query Pledge '[*]))

  (println "ACTIONS AFTER VERIFICATION")
  (println "Note: :action/pledges are now set properly")
  (pprint (dv/query Action '[*])))

(comment
  (dv/query User [:user/email {:user-pledges [:pledge/action]}]))
