cube(`CovidVaccinationByLga`, {
  sql: `SELECT * FROM public.covid_vaccination_by_lga t1 JOIN (SELECT dimensions_id, MAX(saved_date) FROM public.covid_vaccination_by_lga GROUP BY dimensions_id) t2 ON t1.dimensions_id = t2.dimensions_id`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

  joins: {

  },

  measures: {
    firstVaxDose: {
      sql: `vax_1_dose_15+`,
      type: `sum`,
    },
    secondVaxDose: {
      sql: `vax_1_dose_15+`,
      type: `sum`,
    }
  },

  dimensions: {
    lgaCode: {
      sql: `lga_code`,
      type: `string`
    },

    stateName: {
      sql: `state_name`,
      type: `string`
    },

    lhdName: {
      sql: `lhd_name`,
      type: `string`
    },

    lgaName: {
      sql: `lga_name`,
      type: `string`
    },

    lhdCode: {
      sql: `lhd_code`,
      type: `string`
    },

    country: {
      sql: `country`,
      type: `string`
    },

    stateCode: {
      sql: `state_code`,
      type: `string`
    },

    date: {
      sql: `date`,
      type: `time`
    },

    savedDate: {
      sql: `saved_date`,
      type: `time`
    }
  },

  dataSource: `default`
});
