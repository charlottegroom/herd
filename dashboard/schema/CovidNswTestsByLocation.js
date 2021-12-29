cube(`CovidNswTestsByLocation`, {
  sql: `SELECT * FROM public.covid_nsw_tests_by_location t1 JOIN (SELECT dimensions_id, MAX(saved_date) FROM public.covid_nsw_tests_by_location GROUP BY dimensions_id) t2 ON t1.dimensions_id = t2.dimensions_id`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

  joins: {
    CovidNswCasesByLocation: {
      relationship: `hasMany`,
      sql: `${CovidNswTestsByLocation}.lga_code = ${CovidNswCasesByLocation}.lga_code`
    },
    CovidVaccinationByLga: {
      relationship: `hasMany`,
      sql: `${CovidNswTestsByLocation}.lga_code = ${CovidVaccinationByLga}.lga_code`
    },
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
      type: `string`,
    },

    lgaName: {
      sql: `lga_name`,
      type: `string`
    },

    lgaCode: {
      sql: `lga_code`,
      type: `string`,
      primaryKey: true,
    },

    lhdName: {
      sql: `lhd_name`,
      type: `string`
    },

    lhdCode: {
      sql: `lhd_code`,
      type: `string`,
      primaryKey: true,
    },

    stateCode: {
      sql: `state_code`,
      type: `string`,
      primaryKey: true,
    },

    date: {
      sql: `date`,
      type: `time`,
      primaryKey: true,
    }
  },

  dataSource: `default`
});
