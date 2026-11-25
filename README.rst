Nightly knowledge refresh for a hospital agent
=============================================

This sample shows how to add transformations to a knowledge refresh without
writing a separate job runner for each one. HorizonDB AI Pipelines describes
the data work in SQL; ``pg_durable`` runs it independently of the client.
PostgreSQL publishes accepted results as a single, versioned snapshot.
All example hospital pages and plan details are synthetic.

Start with the `PDF writeup <docs/knowledge-refresh-brief.pdf>`_ or the
`quickstart <QUICKSTART.txt>`_. The `sample run <option3/run-output.txt>`_
can be read without a database.

How it works
------------

1. Stage pages and keep their URLs so search results can cite the source.
2. Submit the configured AI pipelines. The default path splits pages into
   searchable passages. Optional pipelines generate embeddings and extract
   plan benefits using registered models.
3. A durable coordinator waits for the jobs. The applet checks the outputs,
   required pages and release generation. A transaction publishes the
   candidate or records the reason for rejecting it. Readers stay on the
   last accepted version until publication.

The `SQL implementation <option3/refresh.sql>`_, `PowerShell runner
<option3/run.ps1>`_, and `read-only inspection queries <option3/inspect.sql>`_
are under ``option3``. See `engineering findings <docs/findings.rst>`_ for
limitations and their workarounds.

The recorded example used PostgreSQL full-text search and real chunk jobs.
It demonstrated rollback and model-job failure, not successful model
inference or production-scale performance. Use an authorized test database:
each execution creates and retains demo objects for inspection.
