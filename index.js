/* eslint-env mocha */
const debug = require("debug");
const { countBy, head, prop, sortBy } = require("ramda");
const assert = require("assert");
const { spy } = require("sinon");
const Countdown = require("countdown-promise");
const { Queue, Worker } = require("../any-queue/lib/bundle.js");

const emptyArray = length => Array.from(Array(length));

module.exports = function testIntegration({ name, persistenceInterface }) {
  const log = debug(`test:integration:${name}`);

  const assertJobsCreated = function assertJobsCreated(jobCount) {
    return persistenceInterface.readJob({}).then(jobs => {
      assert.equal(jobs.length, jobCount, "Some jobs were not created.");
    });
  };

  const assertJobsDone = function assertJobsDone(jobCount) {
    return persistenceInterface.readJob({}).then(jobs => {
      const doneJobs = jobs.filter(j => j.status === "done");
      assert.equal(doneJobs.length, jobCount, "Some jobs have not been done.");
    });
  };

  const assertJobsHandled = function assertJobsHandled(jobCount, handledJobs) {
    const handledData = handledJobs.map(prop("data"));
    const timesHandled = Object.values(countBy(prop("i"))(handledData));
    const wasAnyHandledTwice = timesHandled.some(times => times > 1);

    assert(handledJobs.length >= jobCount, "Some jobs were not handled.");
    assert(!wasAnyHandledTwice, "Some jobs were handled twice.");
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

      const flagJobHandled = spy(() => {
        countdown.count();
      });

      const queue = new Queue({ persistenceInterface, name: "test-queue" });
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
      const queue = new Queue({ persistenceInterface, name: "test-queue" });
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
            persistenceInterface,
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

  const blockersTestCase = function blockersTestCase(workerCount, jobCount) {
    it(
      `Processes ${jobCount} jobs with ${workerCount} workers, observing blockers`,
      testJobBlocker(workerCount, jobCount)
    );
  };

  describe(`Test integration with ${name}`, function() {
    this.timeout(60000);
    // TODO: clean setup
    //before("Set up", () =>
    //persistenceInterface
    //.connect()
    //.then(persistenceInterface.refresh)
    //.then(persistenceInterface.disconnect)
    //);
    afterEach("Refresh", persistenceInterface.refresh);
    afterEach("Disconnect", persistenceInterface.disconnect);

    jobProcessingTestCase(1, 1);
    jobProcessingTestCase(1, 2);
    jobProcessingTestCase(2, 1);
    blockersTestCase(1, 1);
    blockersTestCase(5, 20);
    jobProcessingTestCase(1, 100);
    jobProcessingTestCase(100, 1);
    jobProcessingTestCase(10, 1000);
  });
};
