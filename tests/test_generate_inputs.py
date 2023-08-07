import json
from unittest.mock import patch

import pytest

from WDL import Error
from WDL.CLI import input_template


def test_draft2_wdl(tmp_path):
    p = tmp_path / "test.wdl"
    draft2 = """
    task echo {
        String foo
        String bar
        command {
            echo ${foo} ${bar}
        }
    }

    workflow basic_workflow {
        String baz
        String foo = "foo"
        call echo {
            input: foo = foo
        }  
        call echo as echo2 {
            input: foo =  baz 
        }               
    }
    """
    p.write_text(draft2)
    inputs = input_template(str(p))
    assert json.loads(inputs) == {
        "basic_workflow.baz": "String",
        "basic_workflow.echo.bar": "String",
        "basic_workflow.echo2.bar": "String",
    }


@pytest.mark.parametrize("version_string", ["development", "1.1", "1.0"])
def test_with_input_block(tmp_path, version_string):
    p = tmp_path / "test.wdl"
    body = """
    struct Person {
        String name
        Int? age       
    }
    
    struct Group {
        Person leader
        Array[Person] members 
        Array[String] address
    }
    
    task echo {
        input {
            String foo
            String bar
            Group group
            Person person = object { "name" : "asd" }
            Map[String, Int] score
            Map[String, Person] friends
            Map[String, Map[String, String]] nested_map
            Pair[Person, Person] couples                        
        }
              
        command {
            echo ${foo} ${bar}
        }
    }

    workflow basic_workflow {
        input {
            String baz
            String foo = "foo"    
            File yellow_pages
            Int page
            Float lat
            Boolean closed               
        }                   
        call echo {
            input: foo = foo
        }       
    }
   """

    wdl_content = f"version {version_string}\n{body}"
    p.write_text(wdl_content)
    inputs = input_template(str(p))
    assert json.loads(inputs) == {
        "basic_workflow.baz": "String",
        "basic_workflow.closed": False,
        "basic_workflow.echo.bar": "String",
        "basic_workflow.echo.couples": {
            "left": {"name": "String"},
            "right": {"name": "String"}
        },
        "basic_workflow.echo.friends": {
            "String": {"name": "String"}
        },
        "basic_workflow.echo.group": {
            "address": ["String"],
            "leader": {"name": "String"},
            "members": [{"name": "String"}],
        },
        "basic_workflow.echo.nested_map": {"String": {"String": "String"}},
        "basic_workflow.echo.score": {"String": 42},
        "basic_workflow.lat": 3.14,
        "basic_workflow.page": 42,
        "basic_workflow.yellow_pages": "File",
    }


def test_with_sub_workflow(tmp_path):
    main_wdl = tmp_path / "main.wdl"
    sub_wdl = tmp_path / "sub_wdl.wdl"

    main_content = """
    import "sub_wdl.wdl" as sub
    
    workflow main_workflow {
        call sub.hello_and_goodbye { input: hello_and_goodbye_input = "sub world" }
        output {
            String main_output = hello_and_goodbye.hello_output
        }
    }
    """

    sub_content = """
    task hello {
        String addressee
        command {
            echo "Hello ${addressee}!"
        }
        output {
            String salutation = read_string(stdout())
        }
    }

    task goodbye {
        String addressee
        command {
            echo "Goodbye ${addressee}!4"
        }
        output {
            String salutation = read_string(stdout())
        }
    }

    workflow hello_and_goodbye {
        String hello_and_goodbye_input
        String goodbye_input

        call hello {input: addressee = hello_and_goodbye_input }
        call goodbye {input: addressee = goodbye_input }

        output {
            String hello_output = hello.salutation
            String goodbye_output = goodbye.salutation
        }
    }
    """

    main_wdl.write_text(main_content)
    sub_wdl.write_text(sub_content)
    inputs = input_template(str(main_wdl))
    assert json.loads(inputs) == {
        "main_workflow.hello_and_goodbye.goodbye_input": "String"
    }

def test_incorrect_wld_throws(tmp_path):
    p = tmp_path / "test.wdl"
    wdl_content = """
    version 1.0

    struct Person {
        String name
        Int? age           
   """
    p.write_text(wdl_content)
    with pytest.raises(Error.SyntaxError):
        input_template(str(p))


def test_no_workflow_wdl_dies(tmp_path):
    p = tmp_path / "test.wdl"
    wdl_content = """
    version 1.0
    
    task echo {
        String foo
        String bar
        command {
            echo ${foo} ${bar}
        }
    }"""
    p.write_text(wdl_content)

    with pytest.raises(SystemExit):
        input_template(str(p))
