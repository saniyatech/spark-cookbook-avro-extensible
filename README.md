# spark-cookbook-avro-extensible
### Cookbook to showcase extensible Avro schema and backwards compatibilty

During development of software applications, it is a common concern to insulate against database schema changes.
It is not practical to maintain different versions of code to support differing schema versions. 
This cookbook provides an approach to enable backward code compatibility in the face of evolving schema changes 
that are additive in nature. This cookbook provides a solution using Avro serialization format.