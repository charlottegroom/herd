cube(`CovidAuVaccinationData`, {
  sql: `SELECT * FROM public.covid_au_vaccination_data t1 JOIN (SELECT dimensions_id, MAX(saved_date) FROM public.covid_au_vaccination_data GROUP BY dimensions_id) t2 ON t1.dimensions_id = t2.dimensions_id`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

  joins: {

  },

  measures: {
    firstVaxDose: {
      sql: `vax_1_dose_diff`,
      type: `runningTotal`,
    },
    secondVaxDose: {
      sql: `vax_2_dose_diff`,
      type: `runningTotal`,
    },
    population: {
      sql: `population`,
      type: `max`,
    },
    firstVaxDosePercent: {
      sql: `vax_1_percent`,
      type: `max`,
    },
    secondVaxDosePercent: {
      sql: `vax_2_percent`,
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

    sex: {
      sql: `sex`,
      type: `string`,
    },

    ageGroup: {
      sql: `age_group`,
      type: `string`,
    },

    country: {
      sql: `country`,
      type: `string`,
      primaryKey: true,
    },

    date: {
      sql: `date`,
      type: `time`
    }

  },

  dataSource: `default`
});
