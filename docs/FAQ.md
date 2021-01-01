# FAQ for miniwdl

<img src="https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/docs/miniwdl-logo.png" width="200"  />

[miniwdl](https://github.com/chanzuckerberg/miniwdl/) is a local runner and developer toolkit for
the bioinformatics-focused [Workflow Description Language (WDL)](http://openwdl.org/). 

TIP: If you are new to working with WDL workflow language, you may want to review the open source 'learn-wdl' course' - [link](https://github.com/openwdl/learn-wdl).   

Also there is an embedded short course 'learn-miniwdl' which includes screencasts reviewing the concepts on this page in more detail - [link](https://github.com/openwdl/learn-wdl/tree/master/6_miniwdl_course)

## Common Questions

###  Install and verify miniwdl

- Q: miniwdl won't intall (or work) on my machine
  - verify installation of Python 3.6+ 
  - verify installation of Docker

- Q: miniwdl isn't working on my Mac
  - verify Docker version (17+) and user permission for Docker 
  - you'll first need to override the `TMPDIR` environment variable, e.g. `export TMPDIR=/tmp`
  - to allow Docker containers to mount shared working directories. 
  - Please [file any other issues](https://github.com/chanzuckerberg/miniwdl/issues) that arise!

- Q: the ```miniwdl run``` command won't run my WDL workflow
  - verify that each workflow task runs from a Docker container image
  - define the container image in the WDL script (task section(s))

- Q: what is the quickest way to verify that miniwdl is properly installed
  - run ```miniwdl --help``` and review the list of possible commands

---

### Run WDL workflow with the ```miniwdl run``` command

- Q: what is simplest way to test running a WDL workflow on miniwdl?
  - run ```miniwdl run_self_test``` - it includes an example workflow
  - verify this job returns the ```ok``` and ```done``` messages as expected

- Q: how can I run a quick ```hello.wdl``` WDL workflow?
  - copy [this file](https://github.com/openwdl/learn-wdl/blob/master/6_miniwdl_course/1_hello.wdl) to your miniwdl dev env 
  - run ```miniwdl run hello.wdl```, there are no input parameters used in this quick test

- Q: Where is the job output from miniwdl job?
  - miniwdl produces workflow, task and job logs as well as task output.  
  - See the folder\file structure generated for each job run for these files. 

- Q: What does ```miniwdl run myfile.wdl``` do?
  - If your WDL file defines inputs and outputs
  - then it prints a list of inputs and outputs and notes which inputs are required

- Q: How to do I assign values to input variables when I use miniwdl to run a WDL file?
  - you can assign directly on the command line, i.e. ```miniwdl run myfile.wdl input1=input.bam input2= input.bai, etc...``` 
  - or you can use the ```--input``` flag and assign input values via an ```input.json``` file

- Q: How can I get more information about job execution?
  - run the ```miniwdl run myfile.wdl``` command 
  - use the ```--verbose``` flag

- Q: What is the ```_LAST``` directory?
  - miniwdl generates a symbolic link `_LAST` pointing to the timestamped subdirectory for most recent run
  - and an `out` directory tree containing symbolic links to the output files.

---

### Check WDL syntax with the ```miniwdl check``` command

- Q: When I run ```miniwdl check myfile.wdl``` it doesn't produce any output
  - re-run the command, sometimes it takes a couple of seconds to complete the WDL file parsing

- Q: Do I have to fix all of items listed (warnings) in the results of ```miniwdl check myfile.wdl``` 
  - this command generates both errors (which you optionally fix) and warnings (which you must fix).
  - warnings are shown in red.

---

### Links

* [chanzuckerberg/miniwdl GitHub](https://github.com/chanzuckerberg/miniwdl/) where issues & contributions are welcome
* [openwdl/wdl GitHub](https://github.com/openwdl/wdl) for WDL spec, proposals, and discussion
* [CZI Science Technology](https://chanzuckerberg.com/technology/science/) sponsors this project
