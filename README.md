# DataflowJournaler

File-Based Journaler using TPL Dataflow

### Introduction

Spike of a simple journaling component conceptionally similar to a persistent [Log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) or [EventStore](https://www.geteventstore.com/) which is able to

- persist any (with Json.NET serializable) object to the filesystem in the form of a timestamped event and
- replay all these persisted events for reprocessing.

### This Project 

The current design focuses on simplicity and correctness. The implementation largely depends on

- [TPL Dataflow](https://msdn.microsoft.com/en-us/library/hh228603(v=vs.110).aspx) with its asynchronous data processing pipeline to model an actor based system and
- [Json.NET](http://www.newtonsoft.com/json) for file-based persistance in a simple, human readable format.

### Usage

The obligatory [Hello World](https://github.com/8snit/Spike.DataflowJournaler/blob/192386e2e4ee1b2a5694bfc15281f1e196b21418/Spike.DataflowJournaler.Tests/SmokeTests.cs#L27-L41) example:

```c#
	using (var journal = new Journal(new JournalConfig
    {
        Directory = TestDirectory
    }))
    {
        await journal.AddAsync("Hello");
        await journal.AddAsync(" ");
        await journal.AddAsync("World");
        await journal.AddAsync("!");

        var message = string.Empty;
        var actionBlock = new ActionBlock<string>(s => { message += s; });
        journal.Replay<string>()(actionBlock);
        actionBlock.Completion.Wait();
        Assert.AreEqual("Hello World!", message);
    }
```

### Feedback
Welcome! Just raise an issue or send a pull request.

