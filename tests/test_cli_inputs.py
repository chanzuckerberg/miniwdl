import pytest
from WDL.CLI import create_arg_parser, generate_inputs

def test_generate_inputs_argparser():
    parser = create_arg_parser()
    args = parser.parse_args(
        ["inputs", "test.wdl"]
    )
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
    assert {'baz': 'String', 'foo': 'String', 'echo.bar': 'String', 'echo2.bar': 'String'} == inputs


@pytest.mark.parametrize("version_string", ["development", "1.1", "1.0"])
def test_with_input_block(tmp_path, version_string):
    p = tmp_path / "test.wdl"
    body = """
    task echo {
        input {
            String foo
            String bar
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
    assert {'baz': 'String', 'foo': 'String', 'echo.bar': 'String', 'echo2.bar': 'String'} == inputs
