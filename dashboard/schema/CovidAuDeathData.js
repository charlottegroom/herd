cube(`CovidAuDeathData`, {
  sql: `SELECT * FROM public.covid_au_death_data t1 JOIN (SELECT dimensions_id, MAX(saved_date) FROM public.covid_au_death_data GROUP BY dimensions_id) t2 ON t1.dimensions_id = t2.dimensions_id`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

  measures: {
    deaths: {
      sql: `deaths`,
      type: `count`,
    },
    avgDeathAge: {
      sql: `age`,
      type: `avg`,
    }
  },

  dimensions: {
    ageGroup: {
      sql: `age_group`,
      type: `number`,
    },

    stateName: {
      sql: `state_name`,
      type: `string`,
      primaryKey: true,
    },

    stateCode: {
      sql: `state_code`,
      type: `string`,
      primaryKey: true,
    },

    sex: {
      sql: `sex`,
      type: `string`,
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
