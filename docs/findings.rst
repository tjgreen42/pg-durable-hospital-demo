Engineering findings
====================

Preserve the source URL during chunking
--------------------------------------
The native chunk sink's document number is local to a batch. It is not a
stable source-page key. Joining on text is also unreliable: two pages can
contain the same paragraph. This sample chunks the source rows again in page
context and checks that the two outputs contain the same passages, including
duplicate counts. Supporting explicit source-key and URL passthrough in
``ai.chunk_step()`` would eliminate the extra work. This is a feature
request, not evidence of corrupted data.

Check the release, not just the child job
----------------------------------------
The supplied scrape can be incomplete even when the chunk job processed
every supplied page. Check a list of required pages that is independent of
the scrape. Keep the release decision separate from engine status: a
coordinator can complete successfully by deciding *not* to publish.

Use a generation when publishing
--------------------------------
An active release can change from A to B and back to A during a build.
Checking the current ID alone would let old work undo a rollback. The
publication transaction also compares a monotonically increasing generation.
The manual rollback helper currently checks only the expected active ID;
a production implementation should accept an expected generation for stale
operator commands too.

Model readiness and recovery boundaries
--------------------------------------
Installing the extension does not register deployments. Before running model
mode, check aliases and permissions and make a permitted one-shot call. The
included missing-model exercise uses deliberately absent aliases and expects
both inference jobs to fail without promoting the incomplete version.
Persisted workflow progress is not an exactly-once guarantee for remote
model calls. Successful inference, restart recovery, and billed retries were
not measured in the recorded example.

Operations
----------
This lab creates staging tables and pipeline registrations per release and
keeps them for inspection. Production needs access controls, retention and
a cleanup policy. The runner is manual; it does not schedule a recurring
crawl. The five-minute deadline is a lab setting, not a sizing recommendation.

References: `AI Pipelines
<https://learn.microsoft.com/en-us/azure/horizondb/ai/ai-pipelines>`_ and
`durable functions
<https://learn.microsoft.com/en-us/azure/horizondb/development/durable-functions>`_.
