# any-queue-test

[![npm version](https://img.shields.io/npm/v/any-queue-test.svg)](https://www.npmjs.com/package/any-queue-test)

> Test helper for any-queue connectors

## Install

```
$ npm install --save-dev any-queue-test
```

## Usage

```js
const testIntegration = require("any-queue-test");
const connector = require(".");

testIntegration({
  name: "mysql",
  createPersistenceInterface: () =>
    connector({
      ...connectorConfig
    })
});
```

## API

### testIntegration({ name, createPersistenceInterface })

Runs mocha tests for adapter, using `name` for format and `createPersistenceInterface` to spawn persistence connections.

## License

MIT © Gerardo Munguia
