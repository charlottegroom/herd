cube(`CovidAuData`, {
  sql: `SELECT * FROM public.covid_au_data t1 JOIN (SELECT dimensions_id, MAX(saved_date) FROM public.covid_au_data GROUP BY dimensions_id) t2 ON t1.dimensions_id = t2.dimensions_id`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

  measures: {
    cases: {
      sql: `confirmed`,
      type: `sum`,
    }
  },

  dimensions: {
    stateCode: {
      sql: `state_code`,
      type: `string`,
      primaryKey: true,
    },

    stateName: {
      sql: `state_name`,
      type: `string`
    },

    country: {
      sql: `country`,
      type: `string`,
      primaryKey: true,
    },

    date: {
      sql: `date`,
      type: `time`,
      primaryKey: true,
    },
  },

  dataSource: `default`
});
