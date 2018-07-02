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

{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Sql.Type.Schema where

import Prelude hiding ((&&), (||), not)

import Database.Sql.Type.Names
import Database.Sql.Type.TableProps
import Database.Sql.Type.Scope

import Control.Monad.Except
import Control.Monad.Writer
import Data.Functor.Identity

import qualified Data.HashMap.Strict as HMS

import Data.Maybe (mapMaybe)
-- import Data.Predicate.Class


overWithColumns :: (r a -> s a) -> WithColumns r a -> WithColumns s a
overWithColumns f (WithColumns r cs) = WithColumns (f r) cs


resolvedColumnHasName :: QColumnName f a -> RColumnRef a -> Bool
resolvedColumnHasName (QColumnName _ _ name) (RColumnAlias (ColumnAlias _ name' _)) = name' == name
resolvedColumnHasName (QColumnName _ _ name) (RColumnRef (QColumnName _ _ name')) = name' == name

makeCatalog :: CatalogMap -> Path -> CurrentDatabase -> Catalog
makeCatalog catalog path currentDb = Catalog{..}
  where
    catalogResolveTableNameHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName
            default' = RTableName fqtn (persistentTable [])
            missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
            missingT = Left $ MissingTable oqtn
            tableNameResolved = Right $ TableNameResolved oqtn default'
        case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS, missingT, tableNameResolved] >> pure default'
            Just database ->
                case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> tell [missingS, missingT, tableNameResolved] >> pure default'
                    Just schema -> do
                        case HMS.lookup (QTableName () None tableName) schema of
                            Nothing -> tell [missingT, tableNameResolved] >> pure default'
                            Just table -> do
                                let rtn = RTableName fqtn table
                                tell [Right $ TableNameResolved oqtn rtn]
                                pure rtn

    catalogResolveTableNameHelper _ = error "only call catalogResolveTableNameHelper with fully qualified table name"

    catalogResolveTableName oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) =
        catalogResolveTableNameHelper oqtn

    catalogResolveTableName (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) =
        catalogResolveTableNameHelper $ QTableName tInfo (Just $ inCurrentDb oqsn) tableName

    catalogResolveTableName oqtn@(QTableName tInfo Nothing tableName) = do
        let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                db <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn db
                table <- HMS.lookup (QTableName () None tableName) schema
                let db' = fmap (const tInfo) currentDb
                    fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                    fqtn = QTableName tInfo (pure fqsn) tableName
                pure $ RTableName fqtn table

        case mapMaybe getTableFromSchema path of
            rtn:_ -> do
                tell [Right $ TableNameResolved oqtn rtn]
                pure rtn
            [] -> throwError $ MissingTable oqtn

    -- TODO session schemas should have the name set to the session ID
    catalogResolveSchemaName :: forall a . OQSchemaName a -> CatalogObjectResolver a (FQSchemaName a)
    catalogResolveSchemaName (QSchemaName sInfo (Just db) schemaName schemaType) =
        pure $ QSchemaName sInfo (pure db) schemaName schemaType
    catalogResolveSchemaName oqsn@(QSchemaName _ Nothing _ _) =
        pure $ inCurrentDb oqsn

    catalogHasDatabase databaseName =
        case HMS.member (void databaseName) catalog of
            False -> DoesNotExist
            True -> Exists

    catalogHasSchema schemaName =
        case HMS.lookup currentDb catalog of
            Just db -> case HMS.member (void schemaName) db of
                False -> DoesNotExist
                True -> Exists
            Nothing -> DoesNotExist

    catalogResolveTableRefHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName

        case HMS.lookup (void db) catalog of
            Nothing -> throwError $ MissingDatabase db
            Just database -> case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> throwError $ MissingSchema oqsn
                    Just tables -> do
                        case HMS.lookup (QTableName () None tableName) tables of
                            Nothing -> throwError $ MissingTable oqtn
                            Just table@SchemaMember{..} -> do
                                let makeRColumnRef (QColumnName () None name) = RColumnRef $ QColumnName tInfo (pure fqtn) name
                                    tableRef = RTableRef fqtn table
                                tell [Right $ TableRefResolved oqtn tableRef]
                                pure $ WithColumns tableRef $ makeColumnSet (Just tableRef) $ map makeRColumnRef columnsList

    catalogResolveTableRefHelper _ = error "only call catalogResolveTableRefHelper with fully qualified table name"

    catalogResolveTableRef :: forall a . [(TableAlias a, [ColumnAlias a])] -> OQTableName a -> CatalogObjectResolver a (WithColumns RTableRef a)
    catalogResolveTableRef _ oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) =
        catalogResolveTableRefHelper oqtn

    catalogResolveTableRef _ (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) =
        catalogResolveTableRefHelper $ QTableName tInfo (Just $ inCurrentDb oqsn) tableName

    catalogResolveTableRef boundCTEs oqtn@(QTableName tInfo Nothing tableName) = do
        case filter (resolvedTableHasName oqtn . fst) $ map (\(table, columns) -> (RTableAlias table columns, columns)) boundCTEs of
            [(t, cs)] -> do
                tell [Right $ TableRefResolved oqtn t]
                pure $ WithColumns t $ makeColumnSet (Just t) $ map RColumnAlias cs
            _:_ -> throwError $ AmbiguousTable oqtn
            [] -> do
                let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                        db <- HMS.lookup currentDb catalog
                        schema <- HMS.lookup uqsn db
                        table@SchemaMember{..} <- HMS.lookup (QTableName () None tableName) schema
                        let db' = fmap (const tInfo) currentDb
                            fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                            fqtn = QTableName tInfo (pure fqsn) tableName
                            makeRColumnRef (QColumnName () None name) = RColumnRef $ QColumnName tInfo (pure fqtn) name
                            tableRef = RTableRef fqtn table
                        pure $ WithColumns tableRef $ makeColumnSet (Just tableRef) $ map makeRColumnRef columnsList

                case mapMaybe getTableFromSchema path of
                    table@(WithColumns tableRef _):_ -> do
                        tell [Right $ TableRefResolved oqtn tableRef]
                        pure table
                    [] -> throwError $ MissingTable oqtn

    catalogResolveCreateSchemaName oqsn = do
        fqsn@(QSchemaName _ (Identity db) schemaName schemaType) <- case schemaNameType oqsn of
            NormalSchema -> catalogResolveSchemaName oqsn
            SessionSchema -> error "can't create the session schema"
        existence <- case HMS.lookup (void db) catalog of
            Nothing -> tell [Left $ MissingDatabase db] >> pure DoesNotExist
            Just database -> if HMS.member (QSchemaName () None schemaName schemaType) database
                then pure Exists
                else pure DoesNotExist
        let rcsn = RCreateSchemaName fqsn existence
        tell [Right $ CreateSchemaNameResolved oqsn rcsn]
        pure rcsn

    catalogResolveCreateTableName name = do
        oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db) schemaName schemaType)) tableName) <-
                case name of
                    oqtn@(QTableName _ Nothing _) -> pure $ inHeadOfPath oqtn
                    QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName -> pure $ QTableName tInfo (pure $ inCurrentDb oqsn) tableName
                    _ -> pure name

        let missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
        existence <- case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS] >> pure DoesNotExist
            Just database -> case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                Nothing -> tell [missingS] >> pure DoesNotExist
                Just schema -> if HMS.member (QTableName () None tableName) schema
                     then pure Exists
                     else pure DoesNotExist

        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            rctn = RCreateTableName (QTableName tInfo (pure fqsn) tableName) existence
        tell [Right $ CreateTableNameResolved oqtn rctn]

        pure rctn

    inCurrentDb :: Applicative g => QSchemaName f a -> QSchemaName g a
    inCurrentDb (QSchemaName sInfo _ schemaName schemaType) =
        let db = fmap (const sInfo) currentDb
         in QSchemaName sInfo (pure db) schemaName schemaType

    inHeadOfPath :: Applicative g => QTableName f a -> QTableName g a
    inHeadOfPath (QTableName tInfo _ tableName) =
        let db = fmap (const tInfo) currentDb
            QSchemaName _ None schemaName schemaType = head path
            qsn = QSchemaName tInfo (pure db) schemaName schemaType
         in QTableName tInfo (pure qsn) tableName

    catalogResolveColumnName :: forall a . ColumnSet a -> OQColumnName a -> CatalogObjectResolver a (RColumnRef a)
    catalogResolveColumnName ColumnSet{..} oqcn = do
        case HMS.lookup (void oqcn) columnsInScope of
            Nothing -> throwError $ MissingColumn oqcn
            Just (Left _) -> throwError $ AmbiguousColumn oqcn
            Just (Right alias) -> pure alias

    catalogHasTable tableName =
        let getTableFromSchema uqsn = do
                database <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn database
                pure $ HMS.member tableName schema
         in case any id $ mapMaybe getTableFromSchema path of
            False -> DoesNotExist
            True -> Exists

    overCatalogMap f =
        let (cm, extra) = f catalog
         in seq cm $ (makeCatalog cm path currentDb, extra)

    catalogMap = catalog

    catalogWithPath newPath = makeCatalog catalog newPath currentDb

    catalogWithDatabase = makeCatalog catalog path

defaultSchemaMember :: SchemaMember
defaultSchemaMember = SchemaMember{..}
  where
    tableType = Table
    persistence = Persistent
    columnsList = []
    viewQuery = Nothing

unknownDatabase :: a -> DatabaseName a
unknownDatabase info = DatabaseName info "<unknown>"

unknownSchema :: a -> FQSchemaName a
unknownSchema info = QSchemaName info (pure $ unknownDatabase info) "<unknown>" NormalSchema

unknownTable :: a -> FQTableName a
unknownTable info = QTableName info (pure $ unknownSchema info) "<unknown>"
