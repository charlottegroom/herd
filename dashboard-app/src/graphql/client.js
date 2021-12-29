/* globals window */
import { ApolloClient } from "apollo-client";
import { InMemoryCache } from "apollo-cache-inmemory";
import { SchemaLink } from "apollo-link-schema";
import { makeExecutableSchema } from "graphql-tools";
const cache = new InMemoryCache();
const defaultDashboardItems = [{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.cases\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\",\"dateRange\":\"Yesterday\"}],\"dimensions\":[\"CovidAuData.stateName\"]},\"chartType\":\"table\"}","name":"New Cases","id":"5","layout":"{\"x\":12,\"y\":0,\"w\":6,\"h\":13}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuDeathData.deaths\"],\"timeDimensions\":[{\"dimension\":\"CovidAuDeathData.date\"}],\"dimensions\":[\"CovidAuDeathData.ageGroup\"]},\"chartType\":\"pie\"}","name":"Death by Age Group (Australia)","id":"6","layout":"{\"x\":18,\"y\":10,\"w\":6,\"h\":11}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuDeathData.deaths\"],\"timeDimensions\":[{\"dimension\":\"CovidAuDeathData.date\"}],\"dimensions\":[\"CovidAuDeathData.sex\"]},\"chartType\":\"pie\"}","name":"Death by Sex (Australia)","id":"7","layout":"{\"x\":18,\"y\":0,\"w\":6,\"h\":10}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.cases\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\"}],\"dimensions\":[]},\"chartType\":\"number\"}","name":"Total Cases","id":"9","layout":"{\"x\":0,\"y\":0,\"w\":4,\"h\":3}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuDeathData.avgDeathAge\"],\"timeDimensions\":[{\"dimension\":\"CovidAuDeathData.date\"}],\"dimensions\":[]},\"chartType\":\"number\",\"sessionGranularity\":\"day\"}","name":"Average Age of Death","id":"12","layout":"{\"x\":8,\"y\":0,\"w\":4,\"h\":3}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuDeathData.deaths\"],\"timeDimensions\":[{\"dimension\":\"CovidAuDeathData.date\"}],\"order\":{\"CovidAuDeathData.ageGroup\":\"asc\"},\"dimensions\":[\"CovidAuDeathData.ageGroup\"]},\"chartType\":\"bar\",\"orderMembers\":[{\"id\":\"CovidAuDeathData.deaths\",\"title\":\"Covid Au Death Data Deaths\",\"order\":\"none\"},{\"id\":\"CovidAuDeathData.ageGroup\",\"title\":\"Covid Au Death Data Age Group\",\"order\":\"asc\"},{\"id\":\"CovidAuDeathData.date\",\"title\":\"Covid Au Death Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuDeathData.ageGroup\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Death By Age","id":"14","layout":"{\"x\":12,\"y\":13,\"w\":6,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.deaths\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\"}],\"order\":{}},\"chartType\":\"number\",\"orderMembers\":[{\"id\":\"CovidAuData.deaths\",\"title\":\"Covid Au Data Deaths\",\"order\":\"none\"},{\"id\":\"CovidAuData.date\",\"title\":\"Covid Au Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Total Deaths","id":"15","layout":"{\"x\":4,\"y\":0,\"w\":4,\"h\":3}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.current_hosp\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\",\"granularity\":\"day\",\"dateRange\":\"This year\"}],\"order\":{},\"dimensions\":[\"CovidAuData.stateName\"]},\"chartType\":\"line\",\"orderMembers\":[{\"id\":\"CovidAuData.current_hosp\",\"title\":\"Covid Au Data Current Hosp\",\"order\":\"none\"},{\"id\":\"CovidAuData.stateName\",\"title\":\"Covid Au Data State Name\",\"order\":\"none\"},{\"id\":\"CovidAuData.date\",\"title\":\"Covid Au Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuData.stateName\",\"CovidAuData.date.day\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Hospitalisations","id":"18","layout":"{\"x\":0,\"y\":16,\"w\":12,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.current_icu\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\",\"granularity\":\"day\",\"dateRange\":\"This year\"}],\"order\":{},\"dimensions\":[\"CovidAuData.stateName\"]},\"chartType\":\"line\",\"orderMembers\":[{\"id\":\"CovidAuData.current_icu\",\"title\":\"Covid Au Data Current Icu\",\"order\":\"none\"},{\"id\":\"CovidAuData.stateName\",\"title\":\"Covid Au Data State Name\",\"order\":\"none\"},{\"id\":\"CovidAuData.date\",\"title\":\"Covid Au Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuData.stateName\",\"CovidAuData.date.day\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"ICU","id":"19","layout":"{\"x\":0,\"y\":24,\"w\":12,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.current_vent\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\",\"granularity\":\"day\",\"dateRange\":\"This year\"}],\"order\":{},\"dimensions\":[\"CovidAuData.stateName\"]},\"chartType\":\"line\",\"orderMembers\":[{\"id\":\"CovidAuData.current_vent\",\"title\":\"Covid Au Data Current Vent\",\"order\":\"none\"},{\"id\":\"CovidAuData.stateName\",\"title\":\"Covid Au Data State Name\",\"order\":\"none\"},{\"id\":\"CovidAuData.date\",\"title\":\"Covid Au Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuData.stateName\",\"CovidAuData.date.day\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Ventilated","id":"20","layout":"{\"x\":12,\"y\":21,\"w\":12,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.current_hosp\",\"CovidAuData.current_icu\",\"CovidAuData.current_vent\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\",\"dateRange\":\"Yesterday\"}],\"order\":{},\"dimensions\":[\"CovidAuData.stateName\"]},\"chartType\":\"table\",\"orderMembers\":[{\"id\":\"CovidAuData.current_hosp\",\"title\":\"Covid Au Data Current Hosp\",\"order\":\"none\"},{\"id\":\"CovidAuData.current_icu\",\"title\":\"Covid Au Data Current Icu\",\"order\":\"none\"},{\"id\":\"CovidAuData.current_vent\",\"title\":\"Covid Au Data Current Vent\",\"order\":\"none\"},{\"id\":\"CovidAuData.stateName\",\"title\":\"Covid Au Data State Name\",\"order\":\"none\"},{\"id\":\"CovidAuData.date\",\"title\":\"Covid Au Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuData.stateName\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Current Hospitalisations","id":"21","layout":"{\"x\":0,\"y\":3,\"w\":12,\"h\":13}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuVaccinationData.secondVaxDosePercent\"],\"timeDimensions\":[{\"dimension\":\"CovidAuVaccinationData.date\",\"dateRange\":\"This year\"}],\"order\":{},\"dimensions\":[\"CovidAuVaccinationData.stateName\"]},\"chartType\":\"table\",\"orderMembers\":[{\"id\":\"CovidAuVaccinationData.secondVaxDosePercent\",\"title\":\"Covid Au Vaccination Data Second Vax Dose Percent\",\"order\":\"none\"},{\"id\":\"CovidAuVaccinationData.stateName\",\"title\":\"Covid Au Vaccination Data State Name\",\"order\":\"none\"},{\"id\":\"CovidAuVaccinationData.date\",\"title\":\"Covid Au Vaccination Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuVaccinationData.stateName\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Vax % Rate","id":"22","layout":"{\"x\":0,\"y\":32,\"w\":12,\"h\":14}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.positivityRate\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\",\"granularity\":\"day\",\"dateRange\":\"This quarter\"}],\"order\":{},\"dimensions\":[\"CovidAuData.stateName\"]},\"chartType\":\"line\",\"orderMembers\":[{\"id\":\"CovidAuData.positivityRate\",\"title\":\"Covid Au Data Positivity Rate\",\"order\":\"none\"},{\"id\":\"CovidAuData.stateName\",\"title\":\"Covid Au Data State Name\",\"order\":\"none\"},{\"id\":\"CovidAuData.date\",\"title\":\"Covid Au Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuData.stateName\",\"CovidAuData.date.day\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Positivity Rate","id":"23","layout":"{\"x\":12,\"y\":37,\"w\":12,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"CovidAuData.cases\"],\"timeDimensions\":[{\"dimension\":\"CovidAuData.date\",\"granularity\":\"day\",\"dateRange\":\"This quarter\"}],\"order\":{},\"dimensions\":[\"CovidAuData.stateName\"]},\"chartType\":\"area\",\"orderMembers\":[{\"id\":\"CovidAuData.cases\",\"title\":\"Covid Au Data Cases\",\"order\":\"none\"},{\"id\":\"CovidAuData.stateName\",\"title\":\"Covid Au Data State Name\",\"order\":\"none\"},{\"id\":\"CovidAuData.date\",\"title\":\"Covid Au Data Date\",\"order\":\"none\"}],\"pivotConfig\":{\"x\":[\"CovidAuData.stateName\",\"CovidAuData.date.day\"],\"y\":[\"measures\"],\"fillMissingDates\":true,\"joinDateRange\":false}}","name":"Cases","id":"24","layout":"{\"x\":12,\"y\":29,\"w\":12,\"h\":8}"}];

export const getDashboardItems = () =>
  JSON.parse(window.localStorage.getItem("dashboardItems")) ||
  defaultDashboardItems;

export const setDashboardItems = items =>
  window.localStorage.setItem("dashboardItems", JSON.stringify(items));


const nextId = () => {
  const currentId =
    parseInt(window.localStorage.getItem("dashboardIdCounter"), 10) || 1;
  window.localStorage.setItem("dashboardIdCounter", currentId + 1);
  return currentId.toString();
};

const toApolloItem = i => ({ ...i, __typename: "DashboardItem" });

const typeDefs = `
  type DashboardItem {
    id: String!
    layout: String
    vizState: String
    name: String
  }

  input DashboardItemInput {
    layout: String
    vizState: String
    name: String
  }

  type Query {
    dashboardItems: [DashboardItem]
    dashboardItem(id: String!): DashboardItem
  }

  type Mutation {
    createDashboardItem(input: DashboardItemInput): DashboardItem
    updateDashboardItem(id: String!, input: DashboardItemInput): DashboardItem
    deleteDashboardItem(id: String!): DashboardItem
  }
`;
const schema = makeExecutableSchema({
  typeDefs,
  resolvers: {
    Query: {
      dashboardItems() {
        const dashboardItems = getDashboardItems();
        return dashboardItems.map(toApolloItem);
      },

      dashboardItem(_, { id }) {
        const dashboardItems = getDashboardItems();
        return toApolloItem(dashboardItems.find(i => i.id.toString() === id));
      }
    },
    Mutation: {
      createDashboardItem: (_, { input: { ...item } }) => {
        const dashboardItems = getDashboardItems();
        item = { ...item, id: nextId(), layout: JSON.stringify({}) };
        dashboardItems.push(item);
        setDashboardItems(dashboardItems);
        return toApolloItem(item);
      },
      updateDashboardItem: (_, { id, input: { ...item } }) => {
        const dashboardItems = getDashboardItems();
        item = Object.keys(item)
          .filter(k => !!item[k])
          .map(k => ({
            [k]: item[k]
          }))
          .reduce((a, b) => ({ ...a, ...b }), {});
        const index = dashboardItems.findIndex(i => i.id.toString() === id);
        dashboardItems[index] = { ...dashboardItems[index], ...item };
        setDashboardItems(dashboardItems);
        return toApolloItem(dashboardItems[index]);
      },
      deleteDashboardItem: (_, { id }) => {
        const dashboardItems = getDashboardItems();
        const index = dashboardItems.findIndex(i => i.id.toString() === id);
        const [removedItem] = dashboardItems.splice(index, 1);
        setDashboardItems(dashboardItems);
        return toApolloItem(removedItem);
      }
    }
  }
});
export default new ApolloClient({
  cache,
  link: new SchemaLink({
    schema
  })
});
