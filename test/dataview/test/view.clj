(ns dataview.test.view
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datomic.api :as d]
            [datomic-schema.schema :as ds]
            [dataview.core :as dv]
            [dataview.test.setup :as setup :refer [create-db
                                                   delete-db
                                                   with-conn]]))

(defn define-data-views []
  (def SimpleUser
    (dv/dataview
     {:name   :simple-user
      :schema {:username [:string :indexed]
               :email [:string :unique]}
      :data   '[:find [(pull ?u QUERY) ...]
                :in $ QUERY
                :where [?u :simple-user/username _]]
      :derived-attrs
      {:name-length '[:find ?length .
                      :in $ ?u
                      :where [?u :simple-user/username ?n]
                             [(count ?n) ?length]]}}))

  (def Blog
    (dv/dataview
     {:name   :blog
      :schema {:title :string
               :description :string}
      :data   '[:find [(pull ?u QUERY) ...]
                :in $ QUERY
                :where [?u :blog/title _]]}))

  (defn not-empty?
    [vals]
    (not (empty? vals)))

  (def UserWithJoin
    (dv/dataview
     {:name   :user-with-join
      :schema {:username [:string :indexed]
               :email [:string :unique]
               :blog {:ref Blog}}
      :data   '[:find [(pull ?u QUERY) ...]
                :in $ QUERY
                :where [?u :user-with-join/username _]]
      :derived-attrs
      {:blogger? '[:find (dataview.test.view/not-empty? ?b)
                   :in $ ?u
                   :where [?u :user-with-join/blog ?b]]}}))

  (def UserWithManyJoin
    (dv/dataview
     {:name   :user-with-many-join
      :schema {:username [:string :indexed]
               :email [:string :unique]
               :blogs {[:ref :many] Blog}}
      :data   '[:find [(pull ?u QUERY) ...]
                :in $ QUERY
                :where [?u :user-with-many-join/username _]]
      :derived-attrs
      {:blog-count '[:find (count ?b)
                     :in $ ?u
                     :where [?u :user-with-join/blog ?b]]}}))

  (def UserWithRecursiveJoin
    (dv/dataview
     {:name   :user-with-recursive-join
      :schema {:username [:string :indexed]
               :email [:string :unique]
               :friends {[:ref :many] '...}}
      :data   '[:find [(pull ?u QUERY) ...]
                :in $ QUERY
                :where [?u :user-with-recursive-join/username _]]
      :derived-attrs
      {:popular? '[:find ?num-friends
                   :in $ ?u
                   :where [?u :user-with-recursive-join/friends ?f]
                          [(count ?f) ?num-friends]
                          [(> ?num-friends 1)]]}})))

(defn create-schemas [conn schemas]
  (d/transact conn (concat
                    (ds/generate-parts [(ds/part "test")])
                    (ds/generate-schema schemas))))

(defn define-container []
  (with-conn
    (def Container
      (dv/container {:views #{SimpleUser
                              Blog
                              UserWithJoin
                              UserWithManyJoin
                              UserWithRecursiveJoin}
                     :create-schemas #(create-schemas conn %)
                     :notify #()}))))

(defn setup []
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

;; (with-conn
;;   (let [materializer (dv/materializer {:views [SimpleUser
;;                                                Blog
;;                                                UserWithJoin
;;                                                UserWithManyJoin]
;;                                        :conn conn})]))

(deftest defdataview-satisifes-protocols
  (is (satisfies? dv/IName SimpleUser))
  (is (satisfies? dv/ISchema SimpleUser))
  (is (satisfies? dv/IPullQuery SimpleUser)))

(deftest defdataview-sets-name
  (is (= 'simple-user (dv/name SimpleUser)))
  (is (= 'blog (dv/name Blog)))
  (is (= 'user-with-join (dv/name UserWithJoin)))
  (is (= 'user-with-many-join (dv/name UserWithManyJoin))))

(deftest defdataview-sets-schema
  (is (dv/schema? (dv/schema SimpleUser)))
  (is (dv/schema? (dv/schema Blog)))
  (is (dv/schema? (dv/schema UserWithJoin)))
  (is (dv/schema? (dv/schema UserWithManyJoin))))

(deftest schema->fields
  (is (= [[:foo :string]]
         (dv/schema->fields {:foo :string})))
  (is (= [[:foo :string]
          [:bar :uuid]]
         (dv/schema->fields {:foo :string
                             :bar :uuid})))
  (is (= [[:foo :string :unique]]
         (dv/schema->fields {:foo [:string :unique]}))))

(deftest schema->datomic-schema
  (is (= {:name "foo"
          :basetype :foo
          :namespace "foo"
          :fields {"foo" [:string #{:unique}]}}
         (dv/schema->datomic-schema 'foo {:foo [:string :unique]})))
  (is (= {:name "bar"
          :basetype :bar
          :namespace "bar"
          :fields {"foo" [:string #{}]}}
         (dv/schema->datomic-schema 'bar {:foo :string})))
  (is (= {:name "baz"
          :basetype :baz
          :namespace "baz"
          :fields {"bar" [:ref #{:unique :many}]}}
         (dv/schema->datomic-schema
           'baz {:bar {[:ref :unique :many] SimpleUser}}))))

(deftest schema->pull-query
  (is (= [:db/id :foo/bar :foo/baz]
         (dv/schema->pull-query 'foo {:bar :string
                                      :baz [:boolean :unique]})))
  (is (= [:db/id :organization/name
          {:organization/users
           [:db/id :simple-user/username :simple-user/email]}]
         (dv/schema->pull-query 'organization
                                {:name :string
                                 :users {[:ref :many] SimpleUser}}))))

(deftest pull-query
  (is (= [:db/id :simple-user/username :simple-user/email]
         (dv/pull-query SimpleUser)))
  (is (= [:db/id :blog/title :blog/description]
         (dv/pull-query Blog)))
  (is (= [:db/id :user-with-join/username :user-with-join/email
          {:user-with-join/blog [:db/id :blog/title :blog/description]}]
         (dv/pull-query UserWithJoin)))
  (is (= [:db/id
          :user-with-recursive-join/username
          :user-with-recursive-join/email
          {:user-with-recursive-join/friends '...}]
         (dv/pull-query UserWithRecursiveJoin)))
  (is (= [:db/id
          :user-with-many-join/username
          :user-with-many-join/email
          {:user-with-many-join/blogs
           [:db/id :blog/title :blog/description]}]
         (dv/pull-query UserWithManyJoin))))

(deftest simple-user-queries
  (with-conn
    @(d/transact conn [{:db/id (d/tempid :db.part/test)
                        :simple-user/username "Jeff"
                        :simple-user/email "jeff@jeff.org"}
                       {:db/id (d/tempid :db.part/test)
                        :simple-user/username "Ada"
                        :simple-user/email "ada@ada.org"}])
    (dv/build! SimpleUser conn)
    (is (= #{{:simple-user/username "Jeff"}
             {:simple-user/username "Ada"}}
           (set (dv/query SimpleUser [:simple-user/username]))))
    (is (= #{{:simple-user/email "jeff@jeff.org"}
             {:simple-user/email "ada@ada.org"}}
           (set (dv/query SimpleUser [:simple-user/email]))))
    (is (= #{{:simple-user/username "Jeff"
              :simple-user/email "jeff@jeff.org"}
             {:simple-user/username "Ada"
              :simple-user/email "ada@ada.org"}}
           (set (dv/query SimpleUser [:simple-user/username
                                      :simple-user/email]))))))

(deftest user-with-recursive-join-queries
  (with-conn
    @(d/transact conn [{:db/id (d/tempid :db.part/test -1)
                        :user-with-recursive-join/username "Jeff"
                        :user-with-recursive-join/email "jeff@jeff.org"
                        :user-with-recursive-join/friends
                        [{:db/id (d/tempid :db.part/test -2)}
                         {:db/id (d/tempid :db.part/test -3)}]}
                       {:db/id (d/tempid :db.part/test -2)
                        :user-with-recursive-join/username "Ada"
                        :user-with-recursive-join/email "ada@ada.org"}
                       {:db/id (d/tempid :db.part/test -3)
                        :user-with-recursive-join/username "May"
                        :user-with-recursive-join/email "may@may.org"
                        :user-with-recursive-join/friends
                        {:db/id (d/tempid :db.part/test -2)}}])
    (dv/build! UserWithRecursiveJoin conn)
    (is (= #{{:user-with-recursive-join/username "Jeff"}
             {:user-with-recursive-join/username "Ada"}
             {:user-with-recursive-join/username "May"}}
           (set (dv/query UserWithRecursiveJoin
                          [:user-with-recursive-join/username]))))))
