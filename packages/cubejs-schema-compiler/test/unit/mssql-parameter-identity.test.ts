import { MssqlQuery } from '../../src/adapter/MssqlQuery';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { MysqlQuery } from '../../src/adapter/MysqlQuery';
import { prepareJsCompiler } from './PrepareCompiler';

const model = `cube('companies', {
  sql: "SELECT * FROM (VALUES ('A', 'x'), ('A', NULL), (NULL, 'x'), (NULL, 'x'), ('missing', NULL), ('B', 'y')) AS fixture(company, category)",
  measures: { count: { type: 'count' } },
  dimensions: {
    company: { sql: 'company', type: 'string' },
    category: { sql: 'category', type: 'string' }
  }
});`;

async function makeQuery(QueryClass = MssqlQuery) {
  const compilers = prepareJsCompiler(model);
  await compilers.compiler.compile();
  return new QueryClass(compilers, {
    measures: ['companies.count'],
    dimensions: [{
      expressionName: 'company',
      cubeName: 'companies',
      expression: () => 'COALESCE("companies".company, $0$)',
    }, {
      expressionName: 'category',
      cubeName: 'companies',
      expression: () => 'COALESCE("companies".category, $1$)',
    }],
    expressionParams: ['missing', 'uncategorized'],
    useNativeSqlPlanner: false,
    order: [],
  });
}

describe('MSSQL parameter identity', () => {
  it('keeps SQL API expression params identical in SELECT and GROUP BY', async () => {
    const query = await makeQuery();
    const [sql, params] = query.buildSqlAndParams();
    expect(params).toEqual(['missing', 'uncategorized']);
    expect(sql.match(/COALESCE\("companies"\.company, @_1\)/g)).toHaveLength(2);
    expect(sql.match(/COALESCE\("companies"\.category, @_2\)/g)).toHaveLength(2);
    const [annotated, bindings] = query.buildSqlAndParams(true);
    expect(annotated.match(/\$0\$/g)).toHaveLength(2);
    expect(annotated.match(/\$1\$/g)).toHaveLength(2);
    expect(bindings).toEqual(params);
    expect(query.shouldReuseParams).toBe(true);
  });

  it('reuses indexes, without merging equal independent values or types', async () => {
    const query = await makeQuery();
    const allocator = query.newParamAllocator(['same', 'same', 1, '1', null]);
    const sql = 'SELECT $1$, $0$, $1$, $2$, $3$, $4$, $4$';
    expect(allocator.buildSqlAndParams(sql, false, query.shouldReuseParams)).toEqual([
      'SELECT @_1, @_2, @_1, @_3, @_4, @_5, @_5', ['same', 'same', 1, '1', null]
    ]);
  });

  it('preserves remapped nested references and distinct NULL bindings', async () => {
    const query = await makeQuery();
    // The wrapper remaps subquery-local indexes before passing annotated SQL
    // and the combined expressionParams to the schema compiler.
    const allocator = query.newParamAllocator(['outer', 'inner', null]);
    expect(allocator.buildSqlAndParams(
      'SELECT $0$, (SELECT $1$ FROM (SELECT $2$ AS n) child WHERE $1$ IS NOT NULL), $0$, $2$',
      true, query.shouldReuseParams
    )).toEqual([
      'SELECT $0$, (SELECT $1$ FROM (SELECT $2$ AS n) child WHERE $1$ IS NOT NULL), $0$, $2$',
      ['outer', 'inner', null]
    ]);
  });

  it.each([[PostgresQuery, 'SELECT $1, $1', ['missing']], [MysqlQuery, 'SELECT ?, ?', ['missing', 'missing']]])(
    'retains shared allocator behavior for %p', async (QueryClass, sql, params) => {
      const query = await makeQuery(QueryClass as typeof MssqlQuery);
      expect(query.newParamAllocator(['missing']).buildSqlAndParams('SELECT $0$, $0$', false, query.shouldReuseParams))
        .toEqual([sql, params]);
    }
  );
});
