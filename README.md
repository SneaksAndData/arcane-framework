# Arcane.Framework

The streaming framework for Arcane.Net, a Kubernetes-based Data streaming platform.

This repository contains the toolbox for building Arcane plugins. It provides the necessary interfaces
and classes to build a plugin for the Arcane data streaming platform.

The repository is organized as follows:

- `Configuration/` contains the JSON type converters that can be used to deserialize
                   stream configurations provided to a stream runner by Arcane Operator.

- `Contracts/` contains the Kubernetes annotations and constants that are used for communication
               between the Operator and the stream runner. These values should be identical in both the
               Operator and the stream runner until the
               [StreamClass-based contracts](https://github.com/SneaksAndData/arcane-operator/issues/91)
               are implemented.

- `Providers/` contains the extension methods for the .NET Core host interfaces that are used to
               provide the Arcane services to the stream runner. Please refer to the Arcane developer
               guide for details.

- `Services/` contains the set of classes and interfaces that are used to build the Arcane plugins.

    - `Base/` contains interfaces that define the streaming runner components. In the simpliest case, the a plugin
              should implement the `IStreamContext`, `IStreamContextWriter`, and `IStreamGraphBuilder` interfaces.
              Basic requirements for implementation of these classes are described in the Arcane developer guide.

    - `Sources/` contains a collection of Akka.NET sources that can be used to build the stream graph.

    - `Sinks/` contains a collection of Akka.NET sinks that can be used to build the stream graph.
