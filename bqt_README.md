# BigQuery Tools
This package is a collection of bigquery utilities to help make life easier and faster in Python and Jupyter notebook.
These tools can be used in any python environment but the design decisions are mainly around data analysis and Jupyter.

## Installation
If you're using Science Box (if you don't, consider using it) just add `spotify-bqt` to your `requirements.txt` file and rebuild.

If you don't use Science Box, install using this command:
```sh
pip install --user --index-url=https://pypi.spotify.net/spotify/production --ignore-installed spotify-bqt
```
This is needed because this package is on Spotify's internal repository and is not available publicly.

**Recommendation:** Always pin a specific package when installing using pip, anywhere. It'll save you a lot of headache in the long run. instead of adding `spotify-bqt` add `spotify-bqt==1.0.0` or whatever version you want.

## Contributions
This repo is community owned and maintained so please submit PRs for fixes and new features following best practices for Python (PEP8, tests, examples, ...).

### Package Structure
See [here](https://ghe.spotify.net/science-box/bqt/blob/master/package_structure.md)

### Development Guideline
See [here](https://ghe.spotify.net/science-box/bqt/blob/master/development_guildlines.md)

### Creating PRs and Publishing the Package
1. Add all the necessary code in a new branch
2. Add any new or updated packages to `requirements.txt`
2. Add tests and examples to their respective folders in `./tests`
3. Make sure all new and existing tests pass, `$ py.test --cov=bqt tests/` will run all of them. Also make sure code coverage is more than when you started.
4. Add a new entry to `changelog.md` with a new version number, currently also needs to be added to `bqt/__init__.py` manually but trying automate this
5. Create a new PR and submit for review (feel free to add @chalpert, @slayton or @behrooza as reviewers)
6. Once your code is merged into master, publish the new version to our internal pypi
   1. Switch to master and pull the latest
   2. Create a new tag `git tag X.X.X` and push it `git push --tags`
   3. run `./sp-pypi-upload`

## Bugs and Issues
We use Github Issues to track these, please indicate whether your issue is a bug or feature request in the title and include version info and stack trace/errors you've gotten.

## Tools and Examples
Check out the `/examples` directory for a list of all available tools and example use cases.
