(ns dataview.test.friends
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.rpl.specter :as specter :refer :all]
            [datomic.api :as d]
            [datomic-schema.schema :as ds]
            [dataview.core :as dv]
            [dataview.datomic :as dv-datomic]
            [dataview.test.setup :as setup :refer [create-db
                                                   delete-db
                                                   with-conn
                                                   connect-to-db]]))

(def user
  {;; Schema
   :schema
   {:name 'user
    :attrs {:email  [:string :indexed]
            :friend {[:ref :many] '...}}}
   ;; Sources to fetch the view data from
   :sources
   [;; Main database
    {:type      :main
     :query     '[:find [(pull ?user query-attrs) ...]
                  :in $ query-attrs
                  :where [?user :user/email _]]}
    ;; Derived attribute (users are popular if they have friends)
    {:type      :derived-attr
     :name      :popular?
     :query     '[:find (count ?friend) .
                  :in $ ?user
                  :where [?user :user/friend ?friend]]
     :transform [[ALL] #(and (not (nil? %))
                             (pos? %))]}]})

(defn -main []
  (delete-db)
  (create-db)

  (def overview
    (dv/overview
     {;; Views managed by this overview
      :views [user]
      ;; Database to operate against
      :conn (connect-to-db)
      ;; Function to resolve an entity into its ID
      :entity-id :db/id
      ;; Function to install schemas
      :install-schemas dv-datomic/install-schemas
      ;; Function to compute query attributes from a view
      :query-attrs dv-datomic/query-attrs
      ;; Function execute a query
      :query dv-datomic/query
      ;; Function to parse/apply transformations
      :transform dv/transform-specter
      ;; Function to merge derived attributes
      :merge-attr dv-datomic/merge-attr}))

  (comment
    (def datomic-overview
      (dv-datomic/overview {:views [user]
                             :conn (connect-to-db)
                             :transform dv/transform-specter})))

  (def users (dv/get-view overview 'user))

  (println)
  (println "EMPTY USERS")
  (pprint (dv/query-attrs users))
  (pprint (dv/query users '[*]))

  @(d/transact (connect-to-db)
               [{:db/id (d/tempid :db.part/app -1)
                 :user/email "jeff@jeff.org"}
                {:db/id (d/tempid :db.part/app -2)
                 :user/email "joe@joe.org"}])

  (println)
  (println "USERS")
  (pprint (dv/query users '[*]))

  (println)
  (let [joe (first (dv/query users [:db/id]
                             [['?user :user/email "joe@joe.org"]]))
        jeff (first (dv/query users [:db/id]
                              [['?user :user/email "jeff@jeff.org"]]))]
    (println "JOE " joe)
    (println "JEFF" jeff)
    @(d/transact (connect-to-db)
                 [(-> joe
                      (assoc :user/friend (:db/id jeff))
                      (dissoc :user/popular?))]))

  (println)
  (println "JOE NOW HAS A FRIEND")
  (pprint (dv/query users '[*])))


(comment
  (dv/query users '[*])

  (dt/transact (cond-> []
                 old-pledge (conj (txs/remove-pledge old-pledge))
                 new-props  (conj (txs/update-activist old-props new-props))))

   [[:actions [:db/id :action/pledges] [['?var :db/id 12312312312313]]]
    [:actions [:db/id :action/pledges] [['?var :db/id 985234959345435]]
     [:activists [:db/id :activist/pledge]
      [['?var :activist/email "foo@foo.org"]]]]]



    (dv/query actions (:db-after tx) [:db/id :action/pledges]
              [['?action :db/id 123123123213123]])

    -> [{:action [{:db/id 12312313123213 :action/pledges 1}
                  {:db/id 12302130921830 :action/pledges 0}]}]

)
