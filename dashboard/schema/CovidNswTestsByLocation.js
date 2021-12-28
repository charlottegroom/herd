cube(`CovidNswTestsByLocation`, {
  sql: `SELECT * FROM public.covid_nsw_tests_by_location t1 JOIN (SELECT dimensions_id, MAX(saved_date) FROM public.covid_nsw_tests_by_location GROUP BY dimensions_id) t2 ON t1.dimensions_id = t2.dimensions_id`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

  joins: {

  },

  measures: {
    tests: {
      sql: `tests`,
      type: `sum`,
    }
  },

  dimensions: {
    country: {
      sql: `country`,
      type: `string`
    },

    stateName: {
      sql: `state_name`,
      type: `string`
    },

    lgaName: {
      sql: `lga_name`,
      type: `string`
    },

    lgaCode: {
      sql: `lga_code`,
      type: `string`
    },

    lhdName: {
      sql: `lhd_name`,
      type: `string`
    },

    lhdCode: {
      sql: `lhd_code`,
      type: `string`
    },

    stateCode: {
      sql: `state_code`,
      type: `string`
    },

    savedDate: {
      sql: `saved_date`,
      type: `time`
    },

    date: {
      sql: `date`,
      type: `time`
    }
  },

  dataSource: `default`
});
