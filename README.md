# Template repository for .NET projects

Use this repo to create new python projects with ready-to-go build and deploy pipelines.

## Checklist

Remember to do the following after creating a new repo:

- :heavy_check_mark: Search and replace `dotnet-project` and `dotnet_project` with your desired project name
- :heavy_check_mark: Select build and deploy pipelines to use in `workflows/` dir.
    - Remove dummy code and uncomment real code
    - Remove pipelines you won't need
- :heavy_check_mark: Set up helm chart in .helm directory
- :heavy_check_mark: Rename solution file in root of the repository
- :heavy_check_mark: Rename project file under /src directory in the repository
- :heavy_check_mark: Rename test project file under /test directory in the repository
- :heavy_check_mark: Update the CODEOWNERS file with real code owners
- :heavy_check_mark: Update all versions of actions to the latest release of [github-actions](https://github.com/SneaksAndData/github-actions) repository
- :heavy_check_mark: Update this README.

Happy coding!

