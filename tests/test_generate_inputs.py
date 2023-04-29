import json
from unittest.mock import patch

import pytest

from WDL import Error
from WDL.CLI import create_arg_parser, generate_inputs


def test_generate_inputs_argparser():
    parser = create_arg_parser()
    args = parser.parse_args(["inputs", "test.wdl"])
    assert "inputs" == args.command
    assert "test.wdl" == args.uri


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
    inputs = generate_inputs(str(p))
    assert json.loads(inputs) == {
        "basic_workflow.baz": "String",
        "basic_workflow.foo": "String",
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
    }
    
    task echo {
        input {
            String foo
            String bar
            Group group
            Person person
        }
              
        command {
            echo ${foo} ${bar}
        }
    }

    workflow basic_workflow {
        input {
            String baz
            String foo = "foo"                   
        } 
                   
        call echo {
            input: foo = foo
        }
        call echo as echo2 {
            input: foo =  baz 
        }               
    }
   """

    wdl_content = f"version {version_string}\n{body}"
    p.write_text(wdl_content)
    inputs = generate_inputs(str(p))
    assert json.loads(inputs) == {
        "basic_workflow.baz": "String",
        "basic_workflow.echo.bar": "String",
        "basic_workflow.echo.group": "Group",
        "basic_workflow.echo.person": "Person",
        "basic_workflow.echo2.bar": "String",
        "basic_workflow.echo2.group": "Group",
        "basic_workflow.echo2.person": "Person",
        "basic_workflow.foo": "String",
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
        generate_inputs(str(p))


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
        generate_inputs(str(p))
