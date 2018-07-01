/* eslint-env mocha */
const debug = require("debug");
const { countBy, filter, head, prop, sortBy } = require("ramda");
const assert = require("assert");
const { spy } = require("sinon");
const Countdown = require("countdown-promise");
const anyQueue = require("any-queue");

const emptyArray = length => Array.from(Array(length));

module.exports = function testIntegration({
  Queue = anyQueue.Queue,
  Worker = anyQueue.Worker,
  name,
  createPersistenceInterface
}) {
  const log = debug(`anyqueue:test:integration:${name}`);

  const assertJobsCreated = async function assertJobsCreated(jobCount) {
    const { connect, disconnect, readJob } = await createPersistenceInterface();

    await connect();
    const jobs = await readJob({});
    assert.equal(jobs.length, jobCount, "Some jobs were not created.");
    await disconnect();
  };

  const assertJobsDone = async function assertJobsDone(jobCount) {
    const { connect, disconnect, readJob } = await createPersistenceInterface();

    await connect();
    const jobs = await readJob({});
    const doneJobs = jobs.filter(j => j.status === "done");
    assert.equal(doneJobs.length, jobCount, "Some jobs have not been done.");

    await disconnect();
  };

  const assertJobsHandled = function assertJobsHandled(jobCount, handledJobs) {
    const handledData = handledJobs.map(prop("data"));

    const unhandledJobs = emptyArray(jobCount)
      .map((_, i) => i)
      .filter(i => !handledData.some(data => data.i === i));

    const counted = countBy(prop("i"))(handledData);
    const handledTwice = filter(n => n > 1, counted);

    assert.deepEqual(unhandledJobs, []);
    assert.deepEqual(handledTwice, []);
  };

  const testJobProcessing = function testJobProcessing(workerCount, jobCount) {
    return done => {
      const countdown = Countdown(jobCount);

      countdown.promise
        .then(() => stop())
        .then(() => assertJobsCreated(jobCount))
        .then(() => assertJobsDone(jobCount))
        .then(() => assertJobsHandled(jobCount, flagJobHandled.args.map(head)))
        .then(() => done());

      const flagJobHandled = spy(job => {
        log("handled job", job.id, job.data.i);
        countdown.count();
      });

      const queue = new Queue({
        persistenceInterface: createPersistenceInterface(),
        name: "test-queue"
      });
      const createWorker = () =>
        Worker({
          persistenceInterface: createPersistenceInterface(),
          queueName: "test-queue",
          instructions: flagJobHandled
        });
      const createJob = i => queue.now({ i });
      const workers = emptyArray(workerCount).map(createWorker);
      const createdJobs = emptyArray(jobCount).map((_, i) => createJob(i));

      log(`created ${workers.length} workers and ${createdJobs.length} jobs.`);

      const stop = (() => {
        workers.forEach(w => w.punchIn());
        return () => Promise.all(workers.map(w => w.punchOut()));
      })();
    };
  };

  const testConnectionReuse = function testConnectionReuse(
    workerCount,
    jobCount
  ) {
    return done => {
      const countdown = Countdown(jobCount);

      countdown.promise
        .then(() => stop())
        .then(() => assertJobsCreated(jobCount))
        .then(() => assertJobsDone(jobCount))
        .then(() => assertJobsHandled(jobCount, flagJobHandled.args.map(head)))
        .then(() => done());

      const flagJobHandled = spy(() => {
        countdown.count();
      });

      const persistenceInterface = createPersistenceInterface();

      const queue = new Queue({
        persistenceInterface,
        name: "test-queue"
      });
      const createWorker = () =>
        Worker({
          persistenceInterface,
          queueName: "test-queue",
          instructions: flagJobHandled
        });
      const createJob = i => queue.now({ i });
      const workers = emptyArray(workerCount).map(createWorker);
      const createdJobs = emptyArray(jobCount).map((_, i) => createJob(i));

      log(`created ${workers.length} workers and ${createdJobs.length} jobs.`);

      const stop = (() => {
        workers.forEach(w => w.punchIn());
        return () => Promise.all(workers.map(w => w.punchOut()));
      })();
    };
  };

  const assertHandlingOrder = function assertHandlingOrder(handledJobs) {
    const handledData = handledJobs.map(prop("data"));
    assert.deepEqual(
      handledData,
      sortBy(prop("i"), handledData),
      "Job blockers were not observed."
    );
  };

  const testJobBlocker = function testJobBlocker(workerCount, jobCount) {
    return done => {
      const queue = new Queue({
        persistenceInterface: createPersistenceInterface(),
        name: "test-queue"
      });
      const createJob = i => blockers => queue.now({ i }, { blockers });
      const creatingJobs = emptyArray(jobCount)
        .map((_, i) => createJob(i))
        .reduce(
          (creatingJobs, creator) =>
            creatingJobs.then(jobs =>
              creator(jobs).then(nextJob => jobs.concat(nextJob))
            ),
          Promise.resolve([])
        );

      creatingJobs.then(createdJobs => {
        const countdown = Countdown(jobCount);
        countdown.promise
          .then(() => stop())
          .then(() => assertHandlingOrder(flagJobHandled.args.map(head)))
          .then(done);

        const flagJobHandled = spy(() => {
          countdown.count();
        });

        const createWorker = () =>
          Worker({
            persistenceInterface: createPersistenceInterface(),
            queueName: "test-queue",
            instructions: flagJobHandled
          });
        const workers = emptyArray(workerCount).map(createWorker);

        log(
          `created ${workers.length} workers and ${createdJobs.length} jobs.`
        );

        const stop = (() => {
          workers.forEach(w => w.punchIn());
          return () => Promise.all(workers.map(w => w.punchOut()));
        })();
      });
    };
  };

  const jobProcessingTestCase = function jobProcessingTestCase(
    workerCount,
    jobCount
  ) {
    it(
      `Processes ${jobCount} jobs with ${workerCount} workers`,
      testJobProcessing(workerCount, jobCount)
    );
  };

  const connectionReuseTestCase = function connectionReuseTestCase(
    workerCount,
    jobCount
  ) {
    it(
      `Processes ${jobCount} jobs with ${workerCount} workers, reusing connection`,
      testConnectionReuse(workerCount, jobCount)
    );
  };

  const blockersTestCase = function blockersTestCase(workerCount, jobCount) {
    it(
      `Processes ${jobCount} jobs with ${workerCount} workers, observing blockers`,
      testJobBlocker(workerCount, jobCount)
    );
  };

  describe(`Test integration with ${name}`, function() {
    this.timeout(120000);

    beforeEach("Refresh", async () => {
      const {
        connect,
        disconnect,
        refresh
      } = await createPersistenceInterface();

      await connect();
      await refresh();
      await disconnect();
    });

    jobProcessingTestCase(1, 1);
    jobProcessingTestCase(1, 2);
    jobProcessingTestCase(2, 1);
    jobProcessingTestCase(10, 50);
    connectionReuseTestCase(1, 3);
    blockersTestCase(1, 1);
    blockersTestCase(5, 20);
    jobProcessingTestCase(1, 100);
    jobProcessingTestCase(100, 1);
    jobProcessingTestCase(10, 1000);
  });
};
