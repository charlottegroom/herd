# Cube.js API

Cube.js is an open source analytical API platform. See [documentation](https://cube.dev/docs/introduction) for more details.

## Environment

Set up connection to desired database using the environment variables in the .env file. See more information in the cubejs docs [here](https://cube.dev/docs/config/databases).

## Development

Install dependencies (within the cubejs-api folder):
```sh
$ npm install .
```

Run development server available on [http://localhost:4000](http://localhost:4000).

```sh
$ npm run dev
```

## Schema

The schema of data analytics cubes is defined with the schema folder. See more information in the cubejs docs [here](https://cube.dev/docs/schema/getting-started).

## Deployment

The API Docker container image is deployed on Heroku. See [here](https://real-time-dashboard.cube.dev/deployment) for deployment steps.

The current deployment of the API is at [https://herd-covid19-dashboard-api.herokuapp.com/](https://herd-covid19-dashboard-api.herokuapp.com/).
