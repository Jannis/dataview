(ns dataview.test.view
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datomic.api :as d]
            [dataview.core :as dv :refer [defdataview]]
            [dataview.test.setup :as setup :refer [with-conn]]))

(defn define-data-views []
  (defdataview SimpleUser
    :name   :simple-user
    :schema {:username [:string :indexed]
             :email [:string :unique]})

  (defdataview Blog
    :name   :blog
    :schema {:title :string
             :description :string})

  (defdataview UserWithJoin
    :name   :user-with-join
    :schema {:username [:string :indexed]
             :email [:string :unique]
             :blog {:ref Blog}})

  (defdataview UserWithManyJoin
    :name   :user-with-many-join
    :schema {:username [:string :indexed]
             :email [:string :unique]
             :blogs {[:ref :many] Blog}})

  (defdataview UserWithRecursiveJoin
    :name   :user-with-recursive-join
    :schema {:username [:string :indexed]
             :email [:string :unique]
             :friends {[:ref :many] '...}}))

(defn data-views-fixture [f]
  (define-data-views)
  (f))

(use-fixtures :once data-views-fixture)

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
  (is (= :simple-user (dv/name SimpleUser)))
  (is (= :blog (dv/name Blog)))
  (is (= :user-with-join (dv/name UserWithJoin)))
  (is (= :user-with-many-join (dv/name UserWithManyJoin))))

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
