from typing import Any, List, Optional, NamedTuple, Callable
import WDL.Type as Ty
import WDL.Value as Val
from collections import namedtuple

Function = namedtuple("Function", ["argument_types", "return_type", "F"])
functions = {
    "_negate" : Function(argument_types=[Ty.Boolean()], return_type=Ty.Boolean(),
                            F=lambda x: Val.Boolean(not x.value)),
    "_land" : Function(argument_types=[Ty.Boolean(), Ty.Boolean()], return_type=Ty.Boolean(),
                          F=lambda l,r: Val.Boolean(l.value and r.value)),
    "_lor" : Function(argument_types=[Ty.Boolean(), Ty.Boolean()], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l.value or r.value)),
    "_add" : Function(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value + r.value)),
    "_sub" : Function(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value - r.value)),
    "_mul" : Function(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value * r.value)),
    "_div" : Function(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(int(l.value / r.value))),
    "_rem" : Function(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value % r.value)),
    "_eqeq" : Function(argument_types=[None, None], return_type=Ty.Boolean(),
                          F=lambda l,r: Val.Boolean(l == r)),
    "_neq" : Function(argument_types=[None, None], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l != r)),
    "_lt" : Function(argument_types=[None, None], return_type=Ty.Boolean(),
                        F=lambda l,r: Val.Boolean(l.value < r.value)),
    "_lte" : Function(argument_types=[None, None], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l.value <= r.value)),
    "_gt" : Function(argument_types=[None, None], return_type=Ty.Boolean(),
                        F=lambda l,r: Val.Boolean(l.value > r.value)),
    "_gte" : Function(argument_types=[None, None], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l.value >= r.value)),
    "_get" : Function(argument_types=[None, Ty.Int()], return_type=Ty.Int(), #FIXME
                         F=lambda arr,which: arr.value[which.value])
}
