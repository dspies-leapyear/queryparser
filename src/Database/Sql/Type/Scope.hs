-- Copyright (c) 2017 Uber Technologies, Inc.
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

module Database.Sql.Type.Scope where

import Database.Sql.Type.Names
import Database.Sql.Type.TableProps
import Database.Sql.Type.Unused
import Database.Sql.Type.Query

import Control.Applicative
import Control.Monad.Identity
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Except
import Control.Monad.Writer

import           Data.Aeson
import qualified Data.HashMap.Strict as HMS
import           Data.HashMap.Strict (HashMap)

import Data.List (subsequences)
import Data.Hashable (Hashable)

import Test.QuickCheck

import Data.Data (Data)
import GHC.Generics (Generic)


type AmbiguousTable a = ()  -- at some point, collect a's
type AmbiguousColumn a = ()

data ColumnSet a = ColumnSet
  { relationColumns :: [RColumnRef a]
  , subrelations :: HashMap (OQTableName ()) (Either (AmbiguousTable a) (RTableRef a, [RColumnRef a]))
  , columnsInScope :: HashMap (OQColumnName ()) (Either (AmbiguousColumn a) (RColumnRef a))
  } deriving (Eq, Show, Functor)

ambiguousColumn :: (b ~ Either (AmbiguousColumn a) (RColumnRef a)) => b -> b -> b
ambiguousColumn _ _ = Left ()

ambiguousTable :: (b ~ Either (AmbiguousTable a) (RTableRef a, [RColumnRef a])) => b -> b -> b
ambiguousTable _ _ = Left ()

emptyColumnSet :: ColumnSet a
emptyColumnSet = ColumnSet [] HMS.empty HMS.empty

data Bindings a = Bindings
    { boundCTEs :: [(TableAlias a, [ColumnAlias a])]
    , boundColumns :: ColumnSet a
    }

emptyBindings :: Bindings a
emptyBindings = Bindings [] emptyColumnSet

data SelectScope a = SelectScope
    { bindForHaving :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForWhere :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForOrder :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForGroup :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForNamedWindow :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    }

type FromColumns a = ColumnSet a
type SelectionAliases a = [ColumnAlias a]

data ResolverInfo a = ResolverInfo
    { catalog :: Catalog
    , onCTECollision :: forall x . (x -> x) -> (x -> x)
    , bindings :: Bindings a
    , selectScope :: FromColumns a -> SelectionAliases a -> SelectScope a
    , lcolumnsAreVisibleInLateralViews :: Bool
    }

mapBindings :: (Bindings a -> Bindings a) -> ResolverInfo a -> ResolverInfo a
mapBindings f ResolverInfo{..} = ResolverInfo{bindings = f bindings, ..}

joinColumnSets :: ColumnSet a -> ColumnSet a -> ColumnSet a
joinColumnSets lhs rhs = ColumnSet
    { relationColumns = relationColumns lhs ++ relationColumns rhs
    , subrelations = HMS.unionWith ambiguousTable (subrelations lhs) (subrelations rhs)
    , columnsInScope = HMS.unionWith ambiguousColumn (columnsInScope lhs) (columnsInScope rhs)
    }

class Truncatable a where
    truncations :: a -> [a]

instance Truncatable (QColumnName Maybe a) where
    truncations (QColumnName i maybeTable name) = do
        table <- Nothing : maybe [] (map Just . truncations) maybeTable
        pure $ QColumnName i table name

instance Truncatable (QTableName Maybe a) where
    truncations (QTableName i maybeSchema name) = do
        schema <- Nothing : maybe [] (map Just . truncations) maybeSchema
        pure $ QTableName i schema name

instance Truncatable (QSchemaName Maybe a) where
    truncations (QSchemaName i maybeDatabase name schemaType) = do
        db <- Nothing : maybe [] (pure . Just) maybeDatabase
        pure $ QSchemaName i db name schemaType

makeColumnSet :: Maybe (RTableRef a) -> [RColumnRef a] -> ColumnSet a
makeColumnSet table relationColumns = ColumnSet{..}
  where
    subrelations = HMS.fromList $ case table of
        Nothing -> []
        Just ref@(RTableAlias (TableAlias _ name _) _) -> (, Right (ref, relationColumns)) <$> [QTableName () Nothing name]
        Just ref@(RTableRef fqtn _) -> (, Right (ref, relationColumns)) <$> truncations (void $ fqtnToOQTN fqtn)

    columnsInScope = HMS.fromList $ relationColumns >>= \ref ->
        (, Right ref) <$> case ref of
            RColumnAlias alias -> truncations $ aliasToOQCN alias
            RColumnRef name -> truncations $ overrideTableForOQCN name

    fqtnToOQTN (QTableName ti (Identity (QSchemaName si (Identity (DatabaseName di dname)) sname stype)) tname) =
        QTableName ti (Just (QSchemaName si (Just (DatabaseName di dname)) sname stype)) tname

    aliasToOQCN (ColumnAlias _ columnName _) = case table of
        Nothing -> QColumnName () Nothing columnName
        Just (RTableAlias (TableAlias _ tableName _) _) -> QColumnName () (Just (QTableName () Nothing tableName)) columnName
        Just (RTableRef fqtn _) -> QColumnName () (Just $ void $ fqtnToOQTN fqtn) columnName

    overrideTableForOQCN (QColumnName _ (Identity fqtn) columnName) =
        let oqtn = Just $ void $ fqtnToOQTN fqtn
            oqtn' = case table of
                Nothing -> Nothing
                Just (RTableAlias (TableAlias _ tableName _) _) -> Just $ QTableName () Nothing tableName
                Just (RTableRef fqtn' _) -> Just $ void $ fqtnToOQTN fqtn'
         in QColumnName () (oqtn' <|> oqtn) columnName

bindColumns :: MonadReader (ResolverInfo a) m => ColumnSet a -> m r -> m r
bindColumns columns = local (mapBindings $ \ Bindings{..} -> Bindings{boundColumns = joinColumnSets columns boundColumns, ..})

bindFromColumns :: MonadReader (ResolverInfo a) m => FromColumns a -> m r -> m r
bindFromColumns = bindColumns

bindAliasedColumns :: MonadReader (ResolverInfo a) m => SelectionAliases a -> m r -> m r
bindAliasedColumns selectionAliases = bindColumns $ makeColumnSet Nothing $ map RColumnAlias selectionAliases

bindFromShadowingAliases :: MonadReader (ResolverInfo a) m => FromColumns a -> SelectionAliases a -> m r -> m r
bindFromShadowingAliases fromColumns selectionAliases m = do
    let newColumns = HMS.fromList $ map (\ alias@(ColumnAlias _ name _) -> (QColumnName () Nothing name, Right $ RColumnAlias alias)) selectionAliases
        cs = ColumnSet
                { relationColumns = map RColumnAlias selectionAliases
                , subrelations = subrelations fromColumns
                , columnsInScope = HMS.union (columnsInScope fromColumns) newColumns
                }
    bindColumns cs m

bindAliasesShadowingFrom :: MonadReader (ResolverInfo a) m => FromColumns a -> SelectionAliases a -> m r -> m r
bindAliasesShadowingFrom fromColumns selectionAliases m = do
    let newColumns = HMS.fromList $ map (\ alias@(ColumnAlias _ name _) -> (QColumnName () Nothing name, Right $ RColumnAlias alias)) selectionAliases
        cs = ColumnSet
                { relationColumns = map RColumnAlias selectionAliases
                , subrelations = subrelations fromColumns
                , columnsInScope = HMS.union newColumns $ columnsInScope fromColumns
                }
    bindColumns cs m

data RawNames
deriving instance Data RawNames
instance Resolution RawNames where
    type TableRef RawNames = OQTableName
    type TableName RawNames = OQTableName
    type CreateTableName RawNames = OQTableName
    type DropTableName RawNames = OQTableName
    type SchemaName RawNames = OQSchemaName
    type CreateSchemaName RawNames = OQSchemaName
    type ColumnRef RawNames = OQColumnName
    type NaturalColumns RawNames = Unused
    type UsingColumn RawNames = UQColumnName
    type StarReferents RawNames = Unused
    type OTablishAliases RawNames = OptionalTablishAliases
    type ITablishAliases RawNames = Unused
    type PositionExpr RawNames = Unused
    type ComposedQueryColumns RawNames = Unused

data ResolvedNames
deriving instance Data ResolvedNames
newtype StarColumnNames a = StarColumnNames [(RColumnRef a, ColumnAlias a)]
    deriving (Generic, Data, Eq, Ord, Show, Functor)

newtype ColumnAliasList a = ColumnAliasList [ColumnAlias a]
    deriving (Generic, Data, Eq, Ord, Show, Functor)

instance Resolution ResolvedNames where
    type TableRef ResolvedNames = RTableRef
    type TableName ResolvedNames = RTableName
    type CreateTableName ResolvedNames = RCreateTableName
    type DropTableName ResolvedNames = RDropTableName
    type SchemaName ResolvedNames = FQSchemaName
    type CreateSchemaName ResolvedNames = RCreateSchemaName
    type ColumnRef ResolvedNames = RColumnRef
    type NaturalColumns ResolvedNames = RNaturalColumns
    type UsingColumn ResolvedNames = RUsingColumn
    type StarReferents ResolvedNames = StarColumnNames
    type OTablishAliases ResolvedNames = RTablishAliases
    type ITablishAliases ResolvedNames = RTablishAliases
    type PositionExpr ResolvedNames = Expr ResolvedNames
    type ComposedQueryColumns ResolvedNames = ColumnAliasList

type Resolver r a =
    StateT Integer  -- column alias generation (counts down from -1, unlike parse phase)
        (ReaderT (ResolverInfo a)
            (CatalogObjectResolver a))
               (r a)


data SchemaMember = SchemaMember
    { tableType :: TableType
    , persistence :: Persistence ()
    , columnsList :: [UQColumnName ()]
    , viewQuery :: Maybe (Query ResolvedNames ())  -- this will always be Nothing for tables
    } deriving (Generic, Data, Eq, Ord, Show)

persistentTable :: [UQColumnName ()] -> SchemaMember
persistentTable cols = SchemaMember Table Persistent cols Nothing


type SchemaMap = HashMap (UQTableName ()) SchemaMember
type DatabaseMap = HashMap (UQSchemaName ()) SchemaMap
type CatalogMap = HashMap (DatabaseName ()) DatabaseMap
type Path = [UQSchemaName ()]
type CurrentDatabase = DatabaseName ()

data Catalog = Catalog
    { catalogResolveSchemaName :: forall a . OQSchemaName a -> CatalogObjectResolver a (FQSchemaName a)
    , catalogResolveTableName :: forall a . OQTableName a -> CatalogObjectResolver a (RTableName a)
    , catalogHasDatabase :: DatabaseName () -> Existence
    , catalogHasSchema :: UQSchemaName () -> Existence
    , catalogHasTable :: UQTableName () -> Existence  -- | nb DoesNotExist does not imply that we can't resolve to this name (defaulting)
    , catalogResolveTableRef :: forall a . [(TableAlias a, [ColumnAlias a])] -> OQTableName a -> CatalogObjectResolver a (WithColumns RTableRef a)
    , catalogResolveCreateSchemaName :: forall a . OQSchemaName a -> CatalogObjectResolver a (RCreateSchemaName a)
    , catalogResolveCreateTableName :: forall a . OQTableName a -> CatalogObjectResolver a (RCreateTableName a)
    , catalogResolveColumnName :: forall a . ColumnSet a -> OQColumnName a -> CatalogObjectResolver a (RColumnRef a)
    , overCatalogMap :: forall a . (CatalogMap -> (CatalogMap, a)) -> (Catalog, a)
    , catalogMap :: !CatalogMap
    , catalogWithPath :: Path -> Catalog
    , catalogWithDatabase :: CurrentDatabase -> Catalog
    }

instance Eq Catalog where
    x == y = catalogMap x == catalogMap y

instance Show Catalog where
    show = show . catalogMap


-- returned by methods in Catalog
type CatalogObjectResolver a =
    (ExceptT (ResolutionError a)  -- error
        (Writer [Either (ResolutionError a) (ResolutionSuccess a)])) -- warnings and successes

data ResolutionError a
    = MissingDatabase (DatabaseName a)
    | MissingSchema (OQSchemaName a)
    | MissingTable (OQTableName a)
    | AmbiguousTable (OQTableName a)
    | MissingColumn (OQColumnName a)
    | AmbiguousColumn (OQColumnName a)
    | UnintroducedTable (OQTableName a)
    | UnexpectedTable (FQTableName a)
    | UnexpectedSchema (FQSchemaName a)
    | BadPositionalReference a Int
        deriving (Eq, Show, Functor)

data ResolutionSuccess a
    = TableNameResolved (OQTableName a) (RTableName a)
    | TableNameDefaulted (OQTableName a) (RTableName a)
    | CreateTableNameResolved (OQTableName a) (RCreateTableName a)
    | CreateSchemaNameResolved (OQSchemaName a) (RCreateSchemaName a)
    | TableRefResolved (OQTableName a) (RTableRef a)
    | TableRefDefaulted (OQTableName a) (RTableRef a)
    | ColumnRefResolved (OQColumnName a) (RColumnRef a)
    | ColumnRefDefaulted (OQColumnName a) (RColumnRef a)
        deriving (Eq, Show, Functor)

isGuess :: ResolutionSuccess a -> Bool
isGuess (TableNameResolved _ _) = False
isGuess (TableNameDefaulted _ _) = True
isGuess (CreateTableNameResolved _ _) = False
isGuess (CreateSchemaNameResolved _ _) = False
isGuess (TableRefResolved _ _) = False
isGuess (TableRefDefaulted _ _) = True
isGuess (ColumnRefResolved _ _) = False
isGuess (ColumnRefDefaulted _ _) = True

isCertain :: ResolutionSuccess a -> Bool
isCertain = not . isGuess


data WithColumns r a = WithColumns
    { withColumnsValue :: r a
    , withColumnsColumns :: ColumnSet a
    }

data WithColumnsAndAliases r a = WithColumnsAndAliases (r a) (ColumnSet a) [ColumnAlias a]
data WithColumnsAndOrders r a = WithColumnsAndOrders (r a) (ColumnSet a) [Order ResolvedNames a]

-- R for "resolved"
data RTableRef a
    = RTableRef (FQTableName a) SchemaMember
    | RTableAlias (TableAlias a) [ColumnAlias a]
      deriving (Generic, Data, Show, Eq, Ord, Functor, Foldable, Traversable)

resolvedTableHasName :: QTableName f a -> RTableRef a -> Bool
resolvedTableHasName (QTableName _ _ name) (RTableAlias (TableAlias _ name' _) _) = name' == name
resolvedTableHasName (QTableName _ _ name) (RTableRef (QTableName _ _ name') _) = name' == name

resolvedTableHasSchema :: QSchemaName f a -> RTableRef a -> Bool
resolvedTableHasSchema _ (RTableAlias _ _) = False
resolvedTableHasSchema (QSchemaName _ _ name schemaType) (RTableRef (QTableName _ (Identity (QSchemaName _ _ name' schemaType')) _) _) =
    name == name' && schemaType == schemaType'

resolvedTableHasDatabase :: DatabaseName a -> RTableRef a -> Bool
resolvedTableHasDatabase _ (RTableAlias _ _) = False
resolvedTableHasDatabase (DatabaseName _ name) (RTableRef (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ name')) _ _)) _) _) = name' == name

tablishColumnAliases :: Tablish ResolvedNames a -> [ColumnAlias a]
tablishColumnAliases = \case
  TablishTable _ (RTablishAliases _ columnAliases) _ -> columnAliases
  TablishSubQuery _ (RTablishAliases _ columnAliases) _ -> columnAliases
  TablishJoin _ (RTablishAliases _ columnAliases) _ _ _ _ -> columnAliases
  TablishLateralView _ (RTablishAliases _ columnAliases) _ _ -> columnAliases


data RTableName a = RTableName (FQTableName a) SchemaMember
    deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data RDropTableName a
    = RDropExistingTableName (FQTableName a) SchemaMember
    | RDropMissingTableName (OQTableName a)
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data RCreateTableName a = RCreateTableName (FQTableName a) Existence
                          deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data RCreateSchemaName a = RCreateSchemaName (FQSchemaName a) Existence
                           deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


instance Arbitrary SchemaMember where
    arbitrary = do
        tableType <- arbitrary
        persistence <- arbitrary
        columnsList <- arbitrary
        viewQuery <- pure Nothing  -- TODO holding off til we have arbitrary queries
        pure SchemaMember{..}
    shrink (SchemaMember type_ persistence cols _) =
        [ SchemaMember type_' persistence' cols' Nothing |  -- TODO same
          (type_', persistence', cols') <- shrink (type_, persistence, cols) ]

shrinkHashMap :: (Eq k, Hashable k) => forall v.  HashMap k v -> [HashMap k v]
shrinkHashMap = map HMS.fromList . subsequences . HMS.toList

instance Arbitrary SchemaMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance Arbitrary DatabaseMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance Arbitrary CatalogMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance ToJSON a => ToJSON (RTableRef a) where
    toJSON (RTableRef fqtn _) = object
        [ "tag" .= String "RTableRef"
        , "fqtn" .= fqtn
        ]
    toJSON (RTableAlias alias _) = object
        [ "tag" .= String "RTableAlias"
        , "alias" .= alias
        ]

instance ToJSON a => ToJSON (RTableName a) where
    toJSON (RTableName fqtn _) = object
        [ "tag" .= String "RTableName"
        , "fqtn" .= fqtn
        ]

instance ToJSON a => ToJSON (RDropTableName a) where
    toJSON (RDropExistingTableName fqtn _) = object
        [ "tag" .= String "RDropExistingTableName"
        , "fqtn" .= fqtn
        ]
    toJSON (RDropMissingTableName oqtn) = object
        [ "tag" .= String "RDropMissingTableName"
        , "oqtn" .= oqtn
        ]

instance ToJSON a => ToJSON (RCreateTableName a) where
    toJSON (RCreateTableName fqtn existence) = object
        [ "tag" .= String "RCreateTableName"
        , "fqtn" .= fqtn
        , "existence" .= existence
        ]

instance ToJSON a => ToJSON (RCreateSchemaName a) where
    toJSON (RCreateSchemaName fqsn existence) = object
        [ "tag" .= String "RCreateSchemaName"
        , "fqsn" .= fqsn
        , "existence" .= existence
        ]

instance ToJSON a => ToJSON (StarColumnNames a) where
    toJSON (StarColumnNames cols) = object
        [ "tag" .= String "StarColumnNames"
        , "cols" .= cols
        ]

instance ToJSON a => ToJSON (ColumnAliasList a) where
    toJSON (ColumnAliasList cols) = object
        [ "tag" .= String "ColumnAliasList"
        , "cols" .= cols
        ]
