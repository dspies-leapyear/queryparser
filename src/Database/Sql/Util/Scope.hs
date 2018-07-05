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

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Sql.Util.Scope
    ( runResolverWarn, runResolverWError, runResolverNoWarn
    , WithColumns (..)
    , queryColumnNames
    , resolveStatement, resolveQuery, resolveQueryWithColumns, resolveSelectAndOrders, resolveCTE, resolveInsert
    , resolveInsertValues, resolveDefaultExpr, resolveDelete, resolveTruncate
    , resolveCreateTable, resolveTableDefinition, resolveColumnOrConstraint
    , resolveColumnDefinition, resolveAlterTable, resolveDropTable
    , resolveSelectColumns, resolvedTableHasName, resolvedTableHasSchema
    , resolveSelection, resolveExpr, resolveTableName, resolveDropTableName
    , resolveCreateSchemaName, resolveSchemaName
    , resolveTableRef, resolveColumnName, resolvePartition, resolveSelectFrom
    , resolveTablish, resolveJoinCondition, resolveSelectWhere, resolveSelectTimeseries
    , resolveSelectGroup, resolveSelectHaving, resolveOrder
    , selectionNames, mkTableSchemaMember
    ) where

import Prelude
import Data.List (foldl')
import Data.Set (Set)
import qualified Data.Set as S
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Either (lefts, rights)
import Data.Traversable (traverse)
import Database.Sql.Type

import qualified Data.HashMap.Strict as HMS
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Text.Lazy as TL
import           Data.Text.Lazy (Text)

import Control.Monad.State
import Control.Monad.Reader
import Control.Monad.Writer
import Control.Monad.Except
import Control.Monad.Identity

import Control.Arrow ((&&&))

import Data.Proxy (Proxy (..))


makeResolverInfo :: Dialect d => Proxy d -> Catalog -> ResolverInfo a
makeResolverInfo dialect catalog = ResolverInfo
    { bindings = emptyBindings
    , onCTECollision =
        if shouldCTEsShadowTables dialect
         then \ f x -> f x
         else \ _ x -> x
    , selectScope = getSelectScope dialect
    , lcolumnsAreVisibleInLateralViews = areLcolumnsVisibleInLateralViews dialect
    , ..
    }

makeColumnAlias :: a -> Text -> Resolver ColumnAlias a
makeColumnAlias r alias = ColumnAlias r alias . ColumnAliasId <$> getNextCounter

makeTableAlias :: a -> Text -> Resolver TableAlias a
makeTableAlias r alias = TableAlias r alias . TableAliasId <$> getNextCounter

getNextCounter :: Monad m => StateT Integer m Integer
getNextCounter = modify (subtract 1) >> get

runResolverWarn :: Dialect d => Resolver r a -> Proxy d -> Catalog -> (Either (ResolutionError a) (r a), [Either (ResolutionError a) (ResolutionSuccess a)])
runResolverWarn resolver dialect catalog = runWriter $ runExceptT $ runReaderT (evalStateT resolver 0) $ makeResolverInfo dialect catalog


runResolverWError :: Dialect d => Resolver r a -> Proxy d -> Catalog -> Either [ResolutionError a] ((r a), [ResolutionSuccess a])
runResolverWError resolver dialect catalog =
    let (result, warningsSuccesses) = runResolverWarn resolver dialect catalog
        warnings = lefts warningsSuccesses
        successes = rights warningsSuccesses
     in case (result, warnings) of
            (Right x, []) -> Right (x, successes)
            (Right _, ws) -> Left ws
            (Left e, ws) -> Left (e:ws)


runResolverNoWarn :: Dialect d => Resolver r a -> Proxy d -> Catalog -> Either (ResolutionError a) (r a)
runResolverNoWarn resolver dialect catalog = fst $ runResolverWarn resolver dialect catalog


resolveStatement :: Dialect d => Statement d RawNames a -> Resolver (Statement d ResolvedNames) a
resolveStatement (QueryStmt stmt) = QueryStmt <$> resolveQuery stmt
resolveStatement (InsertStmt stmt) = InsertStmt <$> resolveInsert stmt
resolveStatement (UpdateStmt stmt) = UpdateStmt <$> resolveUpdate stmt
resolveStatement (DeleteStmt stmt) = DeleteStmt <$> resolveDelete stmt
resolveStatement (TruncateStmt stmt) = TruncateStmt <$> resolveTruncate stmt
resolveStatement (CreateTableStmt stmt) = CreateTableStmt <$> resolveCreateTable stmt
resolveStatement (AlterTableStmt stmt) = AlterTableStmt <$> resolveAlterTable stmt
resolveStatement (DropTableStmt stmt) = DropTableStmt <$> resolveDropTable stmt
resolveStatement (CreateViewStmt stmt) = CreateViewStmt <$> resolveCreateView stmt
resolveStatement (DropViewStmt stmt) = DropViewStmt <$> resolveDropView stmt
resolveStatement (CreateSchemaStmt stmt) = CreateSchemaStmt <$> resolveCreateSchema stmt
resolveStatement (GrantStmt stmt) = pure $ GrantStmt stmt
resolveStatement (RevokeStmt stmt) = pure $ RevokeStmt stmt
resolveStatement (BeginStmt info) = pure $ BeginStmt info
resolveStatement (CommitStmt info) = pure $ CommitStmt info
resolveStatement (RollbackStmt info) = pure $ RollbackStmt info
resolveStatement (ExplainStmt info stmt) = ExplainStmt info <$> resolveStatement stmt
resolveStatement (EmptyStmt info) = pure $ EmptyStmt info

resolveQuery :: Query RawNames a -> Resolver (Query ResolvedNames) a
resolveQuery = (withColumnsValue <$>) . resolveQueryWithColumns

resolveQueryWithColumns :: Query RawNames a -> Resolver (WithColumns (Query ResolvedNames)) a
resolveQueryWithColumns (QuerySelect info select) = do
    WithColumnsAndOrders select' columns _  <- resolveSelectAndOrders select []
    pure $ WithColumns (QuerySelect info select') columns
resolveQueryWithColumns (QueryExcept info Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \ (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryExcept info (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryUnion info distinct Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \ (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryUnion info distinct (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryIntersect info Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \ (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryIntersect info (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryWith info [] query) = overWithColumns (QueryWith info []) <$> resolveQueryWithColumns query
resolveQueryWithColumns (QueryWith info (cte:ctes) query) = do
    cte' <- resolveCTE cte
    Catalog{..} <- asks catalog

    let TableAlias _ alias _ = cteAlias cte'

    updateBindings <- fmap ($ local (mapBindings $ bindCTE cte')) $
        case catalogHasTable $ QTableName () None alias of
            Exists -> asks onCTECollision
            DoesNotExist -> pure id

    WithColumns (QueryWith _ ctes' query') columns <- updateBindings $ resolveQueryWithColumns $ QueryWith info ctes query
    pure $ WithColumns (QueryWith info (cte':ctes') query') columns

resolveQueryWithColumns (QueryOrder info orders query) =
    case query of
        QuerySelect i select -> do
            WithColumnsAndOrders select' columns orders' <- resolveSelectAndOrders select orders
            pure $ WithColumns (QueryOrder info orders' (QuerySelect i select')) columns

        _ -> do
            WithColumns query' columns <- resolveQueryWithColumns query
            let names = queryColumnNames query'
                exprs = map (ColumnExpr info . RColumnAlias) names
            orders' <- bindAliasedColumns names $ mapM (resolveOrder $ zip (map void names) exprs) orders
            pure $ WithColumns (QueryOrder info orders' query') columns

resolveQueryWithColumns (QueryLimit info limit query) = overWithColumns (QueryLimit info limit) <$> resolveQueryWithColumns query
resolveQueryWithColumns (QueryOffset info offset query) = overWithColumns (QueryOffset info offset) <$> resolveQueryWithColumns query

bindCTE :: CTE ResolvedNames a -> Bindings a -> Bindings a
bindCTE CTE{..} =
    let columns =
            case cteColumns of
                [] -> queryColumnNames cteQuery
                cs -> cs
        cte = (cteAlias, columns)
     in \ Bindings{..} -> Bindings{boundCTEs = cte:boundCTEs, ..}


selectionNames :: Selection ResolvedNames a -> [ColumnAlias a]
selectionNames (SelectExpr _ aliases _) = aliases
selectionNames (SelectStar _ _ (StarColumnNames referents)) = map snd referents

selectionExprs :: Selection ResolvedNames a -> [Expr ResolvedNames a]
selectionExprs (SelectExpr _ _ expr) = [expr]
selectionExprs (SelectStar info _ (StarColumnNames referents)) = map (ColumnExpr info . fst) referents

queryColumnNames :: Query ResolvedNames a -> [ColumnAlias a]
queryColumnNames (QuerySelect _ Select{selectCols = SelectColumns _ cols}) = cols >>= selectionNames
queryColumnNames (QueryExcept _ (ColumnAliasList cs) _ _) = cs
queryColumnNames (QueryUnion _ _ (ColumnAliasList cs) _ _) = cs
queryColumnNames (QueryIntersect _ (ColumnAliasList cs) _ _) = cs
queryColumnNames (QueryWith _ _ query) = queryColumnNames query
queryColumnNames (QueryOrder _ _ query) = queryColumnNames query
queryColumnNames (QueryLimit _ _ query) = queryColumnNames query
queryColumnNames (QueryOffset _ _ query) = queryColumnNames query

resolveSelectAndOrders :: Select RawNames a -> [Order RawNames a] -> Resolver (WithColumnsAndOrders (Select ResolvedNames)) a
resolveSelectAndOrders Select{..} orders = do
    (selectFrom', columns) <- traverse resolveSelectFrom selectFrom >>= \case
        Nothing -> pure (Nothing, emptyColumnSet)
        Just (WithColumns selectFrom' columns) -> pure (Just selectFrom', columns)

    selectTimeseries' <- traverse (bindColumns columns . resolveSelectTimeseries) selectTimeseries

    maybeBindTimeSlice selectTimeseries' $ do
        selectCols' <- bindColumns columns $ resolveSelectColumns selectCols

        let selectedAliases = selectionNames =<< selectColumnsList selectCols'
            selectedExprs = selectionExprs =<< selectColumnsList selectCols'
            namedExprs = zip (map void selectedAliases) selectedExprs

        SelectScope{..} <- (\ f -> f columns selectedAliases) <$> asks selectScope

        selectHaving' <- bindForHaving $ traverse resolveSelectHaving selectHaving
        selectWhere' <- bindForWhere $ traverse resolveSelectWhere selectWhere
        selectGroup' <- bindForGroup $ traverse (resolveSelectGroup namedExprs) selectGroup
        selectNamedWindow' <- bindForNamedWindow $ traverse resolveSelectNamedWindow selectNamedWindow
        orders' <- bindForOrder $ mapM (resolveOrder namedExprs) orders
        let select = Select { selectCols = selectCols'
                            , selectFrom = selectFrom'
                            , selectWhere = selectWhere'
                            , selectTimeseries = selectTimeseries'
                            , selectGroup = selectGroup'
                            , selectHaving = selectHaving'
                            , selectNamedWindow = selectNamedWindow'
                            , ..
                            }
        pure $ WithColumnsAndOrders select columns orders'
  where
    maybeBindTimeSlice Nothing = id
    maybeBindTimeSlice (Just timeseries) = bindColumns $ makeColumnSet Nothing [RColumnAlias $ selectTimeseriesSliceName timeseries]


resolveCTE :: CTE RawNames a -> Resolver (CTE ResolvedNames) a
resolveCTE CTE{..} = do
    cteQuery' <- resolveQuery cteQuery
    pure $ CTE
        { cteQuery = cteQuery'
        , ..
        }

resolveInsert :: Insert RawNames a -> Resolver (Insert ResolvedNames) a
resolveInsert Insert{..} = do
    insertTable'@(RTableName fqtn _) <- resolveTableName insertTable
    let insertColumns' = fmap (fmap (\uqcn -> RColumnRef $ uqcn { columnNameTable = Identity fqtn })) insertColumns
    insertValues' <- resolveInsertValues insertValues
    pure $ Insert
        { insertTable = insertTable'
        , insertColumns = insertColumns'
        , insertValues = insertValues'
        , ..
        }

resolveInsertValues :: InsertValues RawNames a -> Resolver (InsertValues ResolvedNames) a
resolveInsertValues (InsertExprValues info exprs) = InsertExprValues info <$> mapM (mapM resolveDefaultExpr) exprs
resolveInsertValues (InsertSelectValues query) = InsertSelectValues <$> resolveQuery query
resolveInsertValues (InsertDefaultValues info) = pure $ InsertDefaultValues info
resolveInsertValues (InsertDataFromFile info path) = pure $ InsertDataFromFile info path

resolveDefaultExpr :: DefaultExpr RawNames a -> Resolver (DefaultExpr ResolvedNames) a
resolveDefaultExpr (DefaultValue info) = pure $ DefaultValue info
resolveDefaultExpr (ExprValue expr) = ExprValue <$> resolveExpr expr

resolveUpdate :: Update RawNames a -> Resolver (Update ResolvedNames) a
resolveUpdate Update{..} = do
    updateTable'@(RTableName fqtn schemaMember) <- resolveTableName updateTable

    let uqcns = columnsList schemaMember
        tgtColRefs = map (\uqcn -> RColumnRef $ uqcn { columnNameInfo = tableNameInfo fqtn
                                                     , columnNameTable = Identity fqtn
                                                     }) uqcns

    tgtColAliases <- mapM (freshAliasForColumnRef updateInfo) tgtColRefs
    let tgtColSet = case updateAlias of
            Just alias -> makeColumnSet (Just $ RTableAlias alias tgtColAliases) tgtColRefs
            Nothing -> makeColumnSet (Just $ RTableRef fqtn schemaMember) tgtColRefs

    (updateFrom', srcColSet) <- case updateFrom of
        Just tablish -> do
            WithColumns t cs <- resolveTablish tablish
            pure (Just t, cs)
        Nothing -> return (Nothing, emptyColumnSet)

    updateSetExprs' <- bindColumns srcColSet $
        mapM (\(uqcn, expr) -> (RColumnRef uqcn { columnNameTable = Identity fqtn},) <$> resolveDefaultExpr expr) updateSetExprs

    updateWhere' <- bindColumns (joinColumnSets tgtColSet srcColSet) $ mapM resolveExpr updateWhere

    pure $ Update
        { updateTable = updateTable'
        , updateSetExprs = updateSetExprs'
        , updateFrom = updateFrom'
        , updateWhere = updateWhere'
        , ..
        }

resolveDelete :: forall a . Delete RawNames a -> Resolver (Delete ResolvedNames) a
resolveDelete (Delete info tableName expr) = do
    tableName'@(RTableName fqtn table@SchemaMember{..}) <- resolveTableName tableName
    when (tableType /= Table) $ fail $ "delete only works on tables; can't delete on a " ++ show tableType
    let QTableName tableInfo _ _ = tableName
        columnSet = makeColumnSet (Just $ RTableRef fqtn table) $ map (\ (QColumnName () None column) -> RColumnRef $ QColumnName tableInfo (pure fqtn) column) columnsList
    bindColumns columnSet $ do
        expr' <- traverse resolveExpr expr
        pure $ Delete info tableName' expr'


resolveTruncate :: Truncate RawNames a -> Resolver (Truncate ResolvedNames) a
resolveTruncate (Truncate info name) = do
    name' <- resolveTableName name
    pure $ Truncate info name'


resolveCreateTable :: forall d a . (Dialect d) => CreateTable d RawNames a -> Resolver (CreateTable d ResolvedNames) a
resolveCreateTable CreateTable{..} = do
    createTableName'@(RCreateTableName fqtn _) <- resolveCreateTableName createTableName createTableIfNotExists

    WithColumns createTableDefinition' columns <- resolveTableDefinition fqtn createTableDefinition
    bindColumns columns $ do
        createTableExtra' <- traverse (resolveCreateTableExtra (Proxy :: Proxy d)) createTableExtra
        pure $ CreateTable
            { createTableName = createTableName'
            , createTableDefinition = createTableDefinition'
            , createTableExtra = createTableExtra'
            , ..
            }



mkTableSchemaMember :: [UQColumnName ()] -> SchemaMember
mkTableSchemaMember columnsList = SchemaMember{..}
  where
    tableType = Table
    persistence = Persistent
    viewQuery = Nothing

resolveTableDefinition :: FQTableName a -> TableDefinition d RawNames a -> Resolver (WithColumns (TableDefinition d ResolvedNames)) a
resolveTableDefinition fqtn (TableColumns info cs) = do
    cs' <- mapM resolveColumnOrConstraint cs
    let columns = mapMaybe columnOrConstraintToColumn $ NonEmpty.toList cs'
        table = mkTableSchemaMember $ map (\ c -> c{columnNameInfo = (), columnNameTable = None}) columns
    pure $ WithColumns (TableColumns info cs') $ makeColumnSet (Just $ RTableRef fqtn table) $ map RColumnRef columns
  where
    columnOrConstraintToColumn (ColumnOrConstraintConstraint _) = Nothing
    columnOrConstraintToColumn (ColumnOrConstraintColumn ColumnDefinition{columnDefinitionName = QColumnName columnInfo None name}) =
        Just $ QColumnName columnInfo (pure fqtn) name


resolveTableDefinition _ (TableLike info name) = do
    name' <- resolveTableName name
    pure $ WithColumns (TableLike info name') emptyColumnSet

resolveTableDefinition fqtn (TableAs info cols query) = do
    query' <- resolveQuery query
    let columns = queryColumnNames query'
        table = mkTableSchemaMember $ map toUQCN columns
        toUQCN (ColumnAlias _ cn _) = QColumnName{..}
          where
            columnNameInfo = ()
            columnNameName = cn
            columnNameTable = None
    pure $ WithColumns (TableAs info cols query') $ makeColumnSet (Just $ RTableRef fqtn table) $ map RColumnAlias columns

resolveTableDefinition _ (TableNoColumnInfo info) = do
    pure $ WithColumns (TableNoColumnInfo info) emptyColumnSet


resolveColumnOrConstraint :: ColumnOrConstraint d RawNames a -> Resolver (ColumnOrConstraint d ResolvedNames) a
resolveColumnOrConstraint (ColumnOrConstraintColumn column) = ColumnOrConstraintColumn <$> resolveColumnDefinition column
resolveColumnOrConstraint (ColumnOrConstraintConstraint constraint) = pure $ ColumnOrConstraintConstraint constraint


resolveColumnDefinition :: ColumnDefinition d RawNames a -> Resolver (ColumnDefinition d ResolvedNames) a
resolveColumnDefinition ColumnDefinition{..} = do
    columnDefinitionDefault' <- traverse resolveExpr columnDefinitionDefault
    pure $ ColumnDefinition
        { columnDefinitionDefault = columnDefinitionDefault'
        , ..
        }


resolveAlterTable :: AlterTable RawNames a -> Resolver (AlterTable ResolvedNames) a
resolveAlterTable (AlterTableRenameTable info old new) = do
    old'@(RTableName (QTableName _ (Identity oldSchema@(QSchemaName _ (Identity oldDb@(DatabaseName _ _)) _ oldSchemaType)) _) table) <- resolveTableName old

    let new'@(RTableName (QTableName _ (Identity (QSchemaName _ _ _ newSchemaType)) _) _) = case new of
            QTableName tInfo (Just (QSchemaName sInfo (Just db) s sType)) t ->
                RTableName (QTableName tInfo (pure (QSchemaName sInfo (pure db) s sType)) t) table

            QTableName tInfo (Just (QSchemaName sInfo Nothing s sType)) t ->
                RTableName (QTableName tInfo (pure (QSchemaName sInfo (pure oldDb) s sType)) t) table

            QTableName tInfo Nothing t ->
                RTableName (QTableName tInfo (pure oldSchema) t) table

    case (oldSchemaType, newSchemaType) of
        (NormalSchema, NormalSchema) -> pure ()
        (SessionSchema, SessionSchema) -> pure ()
        (NormalSchema, SessionSchema) -> error "can't rename a table into the session schema"
        (SessionSchema, NormalSchema) -> error "can't rename a table out of the session schema"

    pure $ AlterTableRenameTable info old' new'

resolveAlterTable (AlterTableRenameColumn info table old new) = do
    table' <- resolveTableName table
    pure $ AlterTableRenameColumn info table' old new
resolveAlterTable (AlterTableAddColumns info table columns) = do
    table' <- resolveTableName table
    pure $ AlterTableAddColumns info table' columns


resolveDropTable :: DropTable RawNames a -> Resolver (DropTable ResolvedNames) a
resolveDropTable DropTable{..} = do
    dropTableNames' <- mapM resolveDropTableName dropTableNames
    pure $ DropTable
        { dropTableNames = dropTableNames'
        , ..
        }


resolveCreateView :: CreateView RawNames a -> Resolver (CreateView ResolvedNames) a
resolveCreateView CreateView{..} = do
    createViewName' <- resolveCreateTableName createViewName createViewIfNotExists
    createViewQuery' <- resolveQuery createViewQuery
    pure $ CreateView
        { createViewName = createViewName'
        , createViewQuery = createViewQuery'
        , ..
        }


resolveDropView :: DropView RawNames a -> Resolver (DropView ResolvedNames) a
resolveDropView DropView{..} = do
    dropViewName' <- resolveDropTableName dropViewName
    pure $ DropView
        { dropViewName = dropViewName'
        , ..
        }


resolveCreateSchema :: CreateSchema RawNames a -> Resolver (CreateSchema ResolvedNames) a
resolveCreateSchema CreateSchema{..} = do
    createSchemaName' <- resolveCreateSchemaName createSchemaName createSchemaIfNotExists
    pure $ CreateSchema
        { createSchemaName = createSchemaName'
        , ..
        }


resolveSelectColumns :: SelectColumns RawNames a -> Resolver (SelectColumns ResolvedNames) a
resolveSelectColumns (SelectColumns info selections) = SelectColumns info <$> mapM resolveSelection selections


resolveSelection :: Selection RawNames a -> Resolver (Selection ResolvedNames) a
resolveSelection (SelectStar info Nothing Unused) = do
    ColumnSet{..} <- asks (boundColumns . bindings)
    relationAliases <- mapM (freshAliasForColumnRef info) relationColumns
    pure $ SelectStar info Nothing $ StarColumnNames $ zip (map (const info <$>) relationColumns) relationAliases

resolveSelection (SelectStar info (Just oqtn) Unused) = do
    ColumnSet{..} <- asks (boundColumns . bindings)
    case HMS.lookup (void oqtn) subrelations of
        Nothing -> throwError $ UnintroducedTable oqtn
        Just (Left _) -> throwError $ AmbiguousTable oqtn
        Just (Right (t, cs)) -> do
            as <- mapM (freshAliasForColumnRef info) cs
            pure $ SelectStar info (Just t) $ StarColumnNames $ zip (map (const info <$>) cs) as

resolveSelection (SelectExpr info alias expr) = SelectExpr info alias <$> resolveExpr expr


resolveExpr :: Expr RawNames a -> Resolver (Expr ResolvedNames) a
resolveExpr (BinOpExpr info op lhs rhs) = BinOpExpr info op <$> resolveExpr lhs <*> resolveExpr rhs

resolveExpr (CaseExpr info whens else_) = CaseExpr info <$> mapM resolveWhen whens <*> traverse resolveExpr else_
  where
    resolveWhen (when_, then_) = (,) <$> resolveExpr when_ <*> resolveExpr then_

resolveExpr (UnOpExpr info op expr) = UnOpExpr info op <$> resolveExpr expr
resolveExpr (LikeExpr info op escape pattern expr) = do
    escape' <- traverse (fmap Escape . resolveExpr . escapeExpr) escape
    pattern' <- Pattern <$> resolveExpr (patternExpr pattern)
    expr' <- resolveExpr expr
    pure $ LikeExpr info op escape' pattern' expr'

resolveExpr (ConstantExpr info constant) = pure $ ConstantExpr info constant
resolveExpr (ColumnExpr info column) = ColumnExpr info <$> resolveColumnName column
resolveExpr (InListExpr info list expr) = InListExpr info <$> mapM resolveExpr list <*> resolveExpr expr
resolveExpr (InSubqueryExpr info query expr) = do
    query' <- resolveQuery query
    expr' <- resolveExpr expr
    pure $ InSubqueryExpr info query' expr'

resolveExpr (BetweenExpr info expr start end) =
    BetweenExpr info <$> resolveExpr expr <*> resolveExpr start <*> resolveExpr end

resolveExpr (OverlapsExpr info range1 range2) = OverlapsExpr info <$> resolveRange range1 <*> resolveRange range2
  where
    resolveRange (from, to) = (,) <$> resolveExpr from <*> resolveExpr to

resolveExpr (FunctionExpr info name distinct args params filter' over) =
    FunctionExpr info name distinct <$> mapM resolveExpr args <*> mapM resolveParam params <*> traverse resolveFilter filter' <*> traverse resolveOverSubExpr over
  where
    resolveParam (param, expr) = (param,) <$> resolveExpr expr
    -- T482568: expand named windows on resolve
    resolveOverSubExpr (OverWindowExpr i window) =
      OverWindowExpr i <$> resolveWindowExpr window
    resolveOverSubExpr (OverWindowName i windowName) =
      pure $ OverWindowName i windowName
    resolveOverSubExpr (OverPartialWindowExpr i partWindow) =
      OverPartialWindowExpr i <$> resolvePartialWindowExpr partWindow
    resolveFilter (Filter i expr) =
      Filter i <$> resolveExpr expr

resolveExpr (AtTimeZoneExpr info expr tz) = AtTimeZoneExpr info <$> resolveExpr expr <*> resolveExpr tz
resolveExpr (SubqueryExpr info query) = SubqueryExpr info <$> resolveQuery query
resolveExpr (ArrayExpr info array) = ArrayExpr info <$> mapM resolveExpr array
resolveExpr (ExistsExpr info query) = ExistsExpr info <$> resolveQuery query
resolveExpr (FieldAccessExpr info expr field) = FieldAccessExpr info <$> resolveExpr expr <*> pure field
resolveExpr (ArrayAccessExpr info expr idx) = ArrayAccessExpr info <$> resolveExpr expr <*> resolveExpr idx
resolveExpr (TypeCastExpr info onFail expr type_) = TypeCastExpr info onFail <$> resolveExpr expr <*> pure type_
resolveExpr (VariableSubstitutionExpr info) = pure $ VariableSubstitutionExpr info

resolveOrder :: [(ColumnAlias (), Expr ResolvedNames a)]
             -> Order RawNames a
             -> Resolver (Order ResolvedNames) a
resolveOrder exprs (Order i posOrExpr direction nullPos) =
    Order i <$> resolvePositionOrExpr exprs posOrExpr <*> pure direction <*> pure nullPos

resolveWindowExpr :: WindowExpr RawNames a
                  -> Resolver (WindowExpr ResolvedNames) a
resolveWindowExpr WindowExpr{..} =
  do
    windowExprPartition' <- traverse resolvePartition windowExprPartition
    windowExprOrder' <- mapM (resolveOrder []) windowExprOrder
    pure $ WindowExpr
        { windowExprPartition = windowExprPartition'
        , windowExprOrder = windowExprOrder'
        , ..
        }

resolvePartialWindowExpr :: PartialWindowExpr RawNames a
                         -> Resolver (PartialWindowExpr ResolvedNames) a
resolvePartialWindowExpr PartialWindowExpr{..} =
  do
    partWindowExprOrder' <- mapM (resolveOrder []) partWindowExprOrder
    partWindowExprPartition' <- mapM resolvePartition partWindowExprPartition
    pure $ PartialWindowExpr
        { partWindowExprOrder = partWindowExprOrder'
        , partWindowExprPartition = partWindowExprPartition'
        , ..
        }

resolveNamedWindowExpr :: NamedWindowExpr RawNames a
                       -> Resolver (NamedWindowExpr ResolvedNames) a
resolveNamedWindowExpr (NamedWindowExpr info name window) =
  NamedWindowExpr info name <$> resolveWindowExpr window
resolveNamedWindowExpr (NamedPartialWindowExpr info name partWindow) =
  NamedPartialWindowExpr info name <$> resolvePartialWindowExpr partWindow

resolveTableName :: OQTableName a -> Resolver RTableName a
resolveTableName table = do
    Catalog{..} <- asks catalog
    lift $ lift $ catalogResolveTableName table

resolveCreateTableName :: CreateTableName RawNames a -> Maybe a -> Resolver (CreateTableName ResolvedNames) a
resolveCreateTableName tableName ifNotExists = do
    Catalog{..} <- asks catalog
    tableName'@(RCreateTableName fqtn existence) <- lift $ lift $ catalogResolveCreateTableName tableName

    when ((existence, void ifNotExists) == (Exists, Nothing)) $ tell [ Left $ UnexpectedTable fqtn ]

    pure $ tableName'

resolveDropTableName :: DropTableName RawNames a -> Resolver (DropTableName ResolvedNames) a
resolveDropTableName tableName = do
    (getName <$> resolveTableName tableName)
        `catchError` handleMissing
  where
    getName (RTableName name table) = RDropExistingTableName name table
    handleMissing (MissingTable name) = pure $ RDropMissingTableName name
    handleMissing e = throwError e


resolveCreateSchemaName :: CreateSchemaName RawNames a -> Maybe a -> Resolver (CreateSchemaName ResolvedNames) a
resolveCreateSchemaName schemaName ifNotExists = do
    Catalog{..} <- asks catalog
    schemaName'@(RCreateSchemaName fqsn existence) <- lift $ lift $ catalogResolveCreateSchemaName schemaName
    when ((existence, void ifNotExists) == (Exists, Nothing)) $ tell [ Left $ UnexpectedSchema fqsn ]
    pure schemaName'

resolveSchemaName :: SchemaName RawNames a -> Resolver (SchemaName ResolvedNames) a
resolveSchemaName schemaName = do
    Catalog{..} <- asks catalog
    lift $ lift $ catalogResolveSchemaName schemaName


resolveTableRef :: OQTableName a -> Resolver (WithColumns RTableRef) a
resolveTableRef tableName = do
    ResolverInfo{catalog = Catalog{..}, bindings = Bindings{..}, ..} <- ask
    lift $ lift $ catalogResolveTableRef boundCTEs tableName


resolveColumnName :: forall a . OQColumnName a -> Resolver (RColumnRef) a
resolveColumnName columnName = do
    (Catalog{..}, Bindings{..}) <- asks (catalog &&& bindings)
    lift $ lift $ catalogResolveColumnName boundColumns columnName


resolvePartition :: Partition RawNames a -> Resolver (Partition ResolvedNames) a
resolvePartition (PartitionBy info exprs) = PartitionBy info <$> mapM resolveExpr exprs
resolvePartition (PartitionBest info) = pure $ PartitionBest info
resolvePartition (PartitionNodes info) = pure $ PartitionNodes info


resolveSelectFrom :: SelectFrom RawNames a -> Resolver (WithColumns (SelectFrom ResolvedNames)) a
resolveSelectFrom (SelectFrom info tablishes) = do
    tablishesWithColumns <- mapM resolveTablish tablishes
    let (tablishes', css) = unzip $ map (\ (WithColumns t cs) -> (t, cs)) tablishesWithColumns
    pure $ WithColumns (SelectFrom info tablishes') $ foldl' joinColumnSets emptyColumnSet css


freshAliasForColumnRef :: a -> RColumnRef a -> Resolver ColumnAlias a
freshAliasForColumnRef info = \case
  RColumnAlias (ColumnAlias _ name _) -> makeColumnAlias info name
  RColumnRef (QColumnName _ _ name) -> makeColumnAlias info name

reuseAliasForTableRef :: a -> RTableRef a -> Resolver TableAlias a
reuseAliasForTableRef info = \case
  RTableAlias (TableAlias _ name ident) _ -> pure $ TableAlias info name ident
  RTableRef (QTableName _ _ name) _ -> makeTableAlias info name

resolveTablish :: forall a . Tablish RawNames a -> Resolver (WithColumns (Tablish ResolvedNames)) a
resolveTablish (TablishTable info aliases ref) = do
    WithColumns ref' ColumnSet{..} <- resolveTableRef ref

    (tableAlias, tableName, columnAliases) <-
        case aliases of
            TablishAliasesNone -> do
                t' <- reuseAliasForTableRef info ref'
                cs' <- mapM (freshAliasForColumnRef info) relationColumns
                pure (t', ref', cs')
            TablishAliasesT t -> do
                cs' <- mapM (freshAliasForColumnRef info) relationColumns
                pure (t, RTableAlias t cs', cs')
            TablishAliasesTC t cs -> pure (t, RTableAlias t cs, cs)

    let aliases' = RTablishAliases (Right tableAlias) columnAliases

    pure $ WithColumns (TablishTable info aliases' ref') $ makeColumnSet (Just tableName) $ map RColumnAlias columnAliases


resolveTablish (TablishSubQuery info aliases query) = do
    query' <- resolveQuery query
    let columns = queryColumnNames query'
    (tAlias, cAliases) <-
        case aliases of
            TablishAliasesNone -> do
                subqueryAliasId <- TableAliasId <$> getNextCounter
                pure (Left subqueryAliasId, columns)
            TablishAliasesT t -> do
                pure (Right t, columns)
            TablishAliasesTC t cs -> pure (Right t, cs)

    let tableAlias = case tAlias of
            Left _ -> Nothing
            Right alias -> Just $ RTableAlias alias cAliases

    pure $ WithColumns (TablishSubQuery info (RTablishAliases tAlias cAliases) query') $ makeColumnSet tableAlias $ map RColumnAlias cAliases

resolveTablish (TablishJoin info Unused joinType cond lhs rhs) = do
    WithColumns lhs' lcolumns <- resolveTablish lhs

    -- special case for Presto
    lcolumnsAreVisible <- asks lcolumnsAreVisibleInLateralViews
    let bindForRhs = case (lcolumnsAreVisible, rhs) of
          (True, TablishLateralView{}) -> bindColumns lcolumns
          _ -> id

    WithColumns rhs' rcolumns <- bindForRhs $ resolveTablish rhs

    joinAliasId <- TableAliasId <$> getNextCounter

    WithColumnsAndAliases cond' columns' columnAliases <- resolveJoinCondition info joinType lcolumns rcolumns cond

    let aliases' = RTablishAliases (Left joinAliasId) columnAliases

    pure $ WithColumns (TablishJoin info aliases' joinType cond' lhs' rhs') columns'

resolveTablish (TablishLateralView info aliases LateralView{..} lhs) = do
    (lhs', lcolumns) <- case lhs of
        Nothing -> return (Nothing, emptyColumnSet)
        Just tablish -> do
            WithColumns lhs' lcolumns <- resolveTablish tablish
            return (Just lhs', lcolumns)

    bindColumns lcolumns $ do
        lateralViewExprs' <- mapM resolveExpr lateralViewExprs
        let view = LateralView
                { lateralViewExprs = lateralViewExprs'
                , ..
                }

        defaultColAliases <- concat <$> mapM defaultAliases lateralViewExprs'
        (tableAlias, columnAliases) <- case aliases of
                TablishAliasesNone -> do
                  lateralViewAliasId <- TableAliasId <$> getNextCounter
                  pure (Left lateralViewAliasId, defaultColAliases)
                TablishAliasesT t -> pure (Right t, defaultColAliases)
                TablishAliasesTC t cs -> pure (Right t, cs)

        let aliases' = RTablishAliases tableAlias columnAliases
            rcolumns = [ viewColumns ]
            viewColumns =
                ( either (const Nothing) (Just . RTableAlias) tableAlias
                , map RColumnAlias columnAliases
                )

        pure $ WithColumns (TablishLateralView info aliases' view lhs') $ error "not dealing with LATERAL VIEW now" lcolumns rcolumns
  where
    defaultAliases (FunctionExpr r (QFunctionName _ _ rawName) _ args _ _ _) = do
        let argsLessOne = (length args) - 1

            alias = makeColumnAlias r

            prependAlias :: Text -> Int -> Resolver ColumnAlias a
            prependAlias prefix int = alias $ prefix `TL.append` (TL.pack $ show int)

            name = TL.toLower rawName

            functionSpecificLookups
              | name == "explode" = map alias [ "col", "key", "val" ]
              | name == "inline" = map alias [ "col1", "col2" ]
              | name == "json_tuple" = map (prependAlias "c") $ take argsLessOne [0..]
              | name == "parse_url_tuple" = map (prependAlias "c") $ take argsLessOne [0..]
              | name == "posexplode" = map alias [ "pos", "val" ]
              | name == "stack" =
                  let n = case head args of
                            (ConstantExpr _ (NumericConstant _ nText)) -> read $ TL.unpack nText
                            _ -> argsLessOne -- this should never happen, but if it does, this is a reasonable guess
                      k = argsLessOne
                      len = (k `div` n) + (if k `mod` n == 0 then 0 else 1)
                   in map (prependAlias "col") $ take len [0..]
              | otherwise = []

        sequence functionSpecificLookups

    defaultAliases _ = fail "lateral view must have a FunctionExpr"

realiasColumnSet :: a -> ColumnSet a -> Resolver (WithColumnsAndAliases Unused) a
realiasColumnSet info ColumnSet{..} = do
    aliases <- mapM (freshAliasForColumnRef info) relationColumns
    let substitutions = HMS.fromList $ zip (map void relationColumns) aliases
        substituteRef x = maybe x RColumnAlias $ HMS.lookup (void x) substitutions
    pure $ WithColumnsAndAliases Unused
        ColumnSet
            { relationColumns = map RColumnAlias aliases
            , subrelations = HMS.map (fmap $ fmap $ map substituteRef) subrelations
            , columnsInScope = HMS.map (fmap substituteRef) columnsInScope
            }
        aliases

resolveJoinCondition :: forall a. a -> JoinType a -> ColumnSet a -> ColumnSet a -> JoinCondition RawNames a -> Resolver (WithColumnsAndAliases (JoinCondition ResolvedNames)) a
resolveJoinCondition joinInfo _ lhs rhs = \case  -- TODO fix semi-joins
    JoinOn expr -> do
        let baseColumns = joinColumnSets lhs rhs
        expr' <- bindColumns baseColumns $ resolveExpr expr
        WithColumnsAndAliases Unused columns aliases <- realiasColumnSet joinInfo baseColumns
        pure $ WithColumnsAndAliases (JoinOn expr') columns aliases
    JoinNatural info Unused -> do
        let name (RColumnRef (QColumnName _ _ column)) = column
            name (RColumnAlias (ColumnAlias _ alias _)) = alias
            used = do
                l <- relationColumns lhs
                r <- relationColumns rhs
                if name l == name r
                 then [RUsingColumn l r]
                 else []
        (columns, aliases) <- joinUsing info used
        pure $ WithColumnsAndAliases (JoinNatural info $ RNaturalColumns used) columns aliases

    JoinUsing info (map toOQCN -> using) -> do
        ls <- bindColumns lhs $ mapM resolveColumnName using
        rs <- bindColumns rhs $ mapM resolveColumnName using
        let used = zipWith RUsingColumn ls rs
        (columns, aliases) <- joinUsing info used
        pure $ WithColumnsAndAliases (JoinUsing info used) columns aliases
  where
    toOQCN :: UQColumnName a -> OQColumnName a
    toOQCN (QColumnName info None name) = QColumnName info Nothing name

    remove :: (Functor f, Ord (f ())) => Set (f ()) -> [f a] -> [f a]
    remove xs = filter (\ y -> not $ S.member (void y) xs)

    joinUsing :: a -> [RUsingColumn a] -> StateT Integer (ReaderT (ResolverInfo a) (CatalogObjectResolver a)) (ColumnSet a, [ColumnAlias a])
    joinUsing info used = do
        let substitutions = HMS.fromList $ map (\ (RUsingColumn l r) -> (void r, l) ) used
            (ls, rs) = unzip $ map (\ (RUsingColumn l r) -> (l, r)) used
            lset = S.fromList $ map void ls
            rset = S.fromList $ map void rs
            substituteRef x = fromMaybe x $ HMS.lookup (void x) substitutions

        WithColumnsAndAliases Unused columnSet aliases <- realiasColumnSet info ColumnSet
              { relationColumns = ls ++ remove lset (relationColumns lhs) ++ remove rset (relationColumns rhs)
              , subrelations = HMS.unionWith ambiguousTable (subrelations lhs) $ HMS.map (fmap $ fmap $ map substituteRef) (subrelations rhs)
              , columnsInScope = HMS.unionWith ambiguousColumn (columnsInScope lhs) (columnsInScope rhs)
              }

        let shadowing = HMS.fromList $ map (\ alias@(ColumnAlias _ name _) -> (QColumnName () Nothing name, Right $ RColumnAlias alias)) aliases

        pure ( columnSet { columnsInScope = HMS.union shadowing (columnsInScope columnSet) }
             , aliases
             )

resolveSelectWhere :: SelectWhere RawNames a -> Resolver (SelectWhere ResolvedNames) a
resolveSelectWhere (SelectWhere info expr) = SelectWhere info <$> resolveExpr expr

resolveSelectTimeseries :: SelectTimeseries RawNames a -> Resolver (SelectTimeseries ResolvedNames) a
resolveSelectTimeseries SelectTimeseries{..} = do
    selectTimeseriesPartition' <- traverse resolvePartition selectTimeseriesPartition
    selectTimeseriesOrder' <- resolveExpr selectTimeseriesOrder
    pure $ SelectTimeseries
        { selectTimeseriesPartition = selectTimeseriesPartition'
        , selectTimeseriesOrder = selectTimeseriesOrder'
        , ..
        }

resolvePositionOrExpr :: [(ColumnAlias (), Expr ResolvedNames a)] -> PositionOrExpr RawNames a -> Resolver (PositionOrExpr ResolvedNames) a
resolvePositionOrExpr exprs (PositionOrExprExpr expr) =
    PositionOrExprExpr . replaceAlias <$> resolveExpr expr
  where
    replaceAlias = \case
      ColumnExpr _ (RColumnAlias alias)
        | Just expr' <- lookup (void alias) exprs -> expr'
      expr' -> expr'

resolvePositionOrExpr exprs (PositionOrExprPosition info pos Unused)
    | pos < 1 = throwError $ BadPositionalReference info pos
    | otherwise =
        case drop (pos - 1) exprs of
            (_, expr):_ -> pure $ PositionOrExprPosition info pos expr
            [] -> throwError $ BadPositionalReference info pos

resolveGroupingElement :: [(ColumnAlias (), Expr ResolvedNames a)] -> GroupingElement RawNames a -> Resolver (GroupingElement ResolvedNames) a
resolveGroupingElement exprs (GroupingElementExpr info posOrExpr) =
    GroupingElementExpr info <$> resolvePositionOrExpr exprs posOrExpr
resolveGroupingElement _ (GroupingElementSet info exprs) =
    GroupingElementSet info <$> mapM resolveExpr exprs

resolveSelectGroup :: [(ColumnAlias (), Expr ResolvedNames a)] -> SelectGroup RawNames a -> Resolver (SelectGroup ResolvedNames) a
resolveSelectGroup exprs SelectGroup{..} = do
    selectGroupGroupingElements' <- mapM (resolveGroupingElement exprs) selectGroupGroupingElements
    pure $ SelectGroup
        { selectGroupGroupingElements = selectGroupGroupingElements'
        , ..
        }

resolveSelectHaving :: SelectHaving RawNames a -> Resolver (SelectHaving ResolvedNames) a
resolveSelectHaving (SelectHaving info exprs) = SelectHaving info <$> mapM resolveExpr exprs

resolveSelectNamedWindow :: SelectNamedWindow RawNames a
                         -> Resolver (SelectNamedWindow ResolvedNames) a
resolveSelectNamedWindow (SelectNamedWindow info windows) =
  SelectNamedWindow info <$> mapM resolveNamedWindowExpr windows
