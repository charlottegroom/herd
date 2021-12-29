cube(`CovidAuData`, {
  sql: `SELECT * FROM public.covid_au_data t1 JOIN (SELECT dimensions_id, MAX(saved_date) FROM public.covid_au_data GROUP BY dimensions_id) t2 ON t1.dimensions_id = t2.dimensions_id`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

  joins: {
    CovidAuDeathData: {
      relationship: `hasMany`,
      sql: `${CovidAuData}.state_code = ${CovidAuDeathData}.state_code`
    },
    CovidAuVaccinationData: {
      relationship: `hasMany`,
      sql: `${CovidAuData}.state_code = ${CovidAuVaccinationData}.state_code`
    },
  },

  measures: {
    cases: {
      sql: `confirmed`,
      type: `sum`,
    },
    total_cases: {
      sql: `confirmed`,
      type: `runningTotal`,
    },
    deaths: {
      sql: `deaths`,
      type: `sum`,
    },
    total_deaths: {
      sql: `deaths`,
      type: `runningTotal`,
    },
    tests: {
      sql: `tests`,
      type: `sum`,
    },
    total_tests: {
      sql: `tests`,
      type: `runningTotal`,
    },
    positivityRate: {
      sql: `${cases}/NULLIF(${tests},0) * 100`,
      type: `number`,
      format: `percent`,
    },
    hosp: {
      sql: `hosp`,
      type: `sum`,
    },
    current_hosp: {
      sql: `hosp_cum`,
      type: `max`,
    },
    icu: {
      sql: `icu`,
      type: `sum`,
    },
    current_icu: {
      sql: `icu_cum`,
      type: `max`,
    },
    vent: {
      sql: `vent`,
      type: `sum`,
    },
    current_vent: {
      sql: `vent_cum`,
      type: `max`,
    },
  },

  dimensions: {
    stateCode: {
      sql: `state_code`,
      type: `string`,
      primaryKey: true,
    },

    stateName: {
      sql: `state_name`,
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
