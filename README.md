# DataflowJournaler

a file-based journaler engine using TPL Dataflow

### Introduction

A simple journaling component (aka [EventStore](https://www.geteventstore.com/) or just [Log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) able to

- persist any with Json.NET serializable object to the filesystem and
- replay all persisted objects within a certain time range.

### This Project 

The current design focuses on simplicity and correctness. The implementation largely depends on

- [TPL Dataflow](https://msdn.microsoft.com/en-us/library/hh228603(v=vs.110).aspx) with its asynchronous data processing pipeline to model an actor based system and
- [Json.NET](http://www.newtonsoft.com/json) for file-based persistance in a simple, human readable format.

### Feedback
Welcome! Just raise an issue or send a pull request.

