from drunc.fsm.interface_factory import FSMInterfaceFactory
from typing import List, Set, Dict, Tuple
from inspect import signature, Parameter
import drunc.fsm.fsm_errors as fsme
from drunc.utils.conf_types import ConfTypeNotSupported, ConfTypes


class FSMInterface:
    '''Abstract class defining a generic interface'''
    def __init__(self, name):
        self.name = name


from druncschema.controller_pb2 import Argument

class Transition:
    def __init__(self, name, source, destination, arguments:[Argument]=[], help:str=''):
        self.source = source
        self.destination = destination
        self.name = name
        self.arguments = arguments
        self.help = help

    def __eq__(self, another):
        same_name = hasattr(another, 'name') and self.name == another.name
        same_destination = hasattr(another, 'destination') and self.destination == another.destination
        same_source = hasattr(another, 'source') and self.source == another.source
        return same_name and same_destination and same_source

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return f'\"{self.name}\": \"{self.source}\" → \"{self.destination}\"'

class Callback:
    def __init__(self, method, mandatory=True):
        self.method = method
        self.mandatory = mandatory


class PreOrPostTransitionSequence:
    def __init__(self, transition:Transition, pre_or_post = "pre"):
        self.transition = transition
        if pre_or_post not in ['pre', 'post']:
            raise RuntimeError(f"pre_or_post should be either 'pre' of 'post', provided {pre_or_post}")

        self.prefix = pre_or_post

        self.sequence = []
        from logging import getLogger
        self._log = getLogger()

    def add_callback(self, interface, mandatory=True):
        method = getattr(interface, f'{self.prefix}_{self.transition.name}')

        if not method:
                raise RuntimeError(f'{self.prefix}_{self.transition.name} method not found in {interface.name}')

        self.sequence += [
            Callback(
                method = method,
                mandatory = mandatory,
            )
        ]


    def execute(self, transition_data, transition_args):
        self._log.debug(f'{transition_data=}, {transition_args=}')
        import json
        if not transition_data:
            transition_data = '{}'

        try:
            input_data = json.loads(transition_data)
        except:
            raise fsme.TransitionDataOfIncorrectFormat(transition_data)

        for callback in self.sequence:
            try:
                self._log.debug(f'data before callback: {input_data}')
                input_data = callback.method(_input_data=input_data, **transition_args)
                self._log.debug(f'data after callback: {input_data}')
            except Exception as e:
                if callback.mandatory:
                    raise e
                else:
                    self._log.error(e)

        self._log.debug(f'data returned: {input_data}')

        return json.dumps(input_data)

    def get_arguments(self):
        '''
        Creates a list of arguments
        This is a bit sloppy, as really, I shouldn't be using protobuf here, and convert them later, but...
        '''
        retr = []
        all_the_parameter_names = []

        for callback in self.sequence:
            method = callback.method
            s = signature(method)

            for pname, p in s.parameters.items():

                if pname in ["_input_data", "args", "kwargs"]:
                    continue

                if pname in all_the_parameter_names:
                    raise RuntimeError(f"Parameter {pname} is already in the list of parameters")
                all_the_parameter_names.append(p)

                default_value = ''

                t = Argument.Type.INT
                from druncschema.generic_pb2 import string_msg, float_msg, int_msg, bool_msg
                from drunc.utils.grpc_utils import pack_to_any

                if p.annotation is str:
                    t = Argument.Type.STRING

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(string_msg(value = p.default))

                elif p.annotation is float:
                    t = Argument.Type.FLOAT

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(float_msg(value = p.default))

                elif p.annotation is int:
                    t = Argument.Type.INT

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(int_msg(value = p.default))

                elif p.annotation is bool:
                    t = Argument.Type.BOOL

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(bool_msg(value = p.default))
                else:
                    raise RuntimeError(f'Annotation {p.annotation} is not handled.')

                a = Argument(
                    name = p.name,
                    presence = Argument.Presence.MANDATORY if p.default == Parameter.empty else Argument.Presence.OPTIONAL,
                    type = t,
                    help = '',
                )

                if default_value:
                    a.default_value.CopyFrom(default_value)

                retr += [a]

        return retr


import abc
class FSMConfigParser(abc.ABC):
    # these 2 need to be filled at init
    pre_transitions = {}
    post_transitions = {}

    @abc.abstractmethod
    def get_initial_state(self):
        pass

    @abc.abstractmethod
    def get_states(self):
        pass

    @abc.abstractmethod
    def get_transitions(self):
        pass


    def get_pre_transition_sequence(self, transition:Transition):
        return self.pre_transitions.get(transition, PreOrPostTransitionSequence(transition, 'pre'))

    def get_post_transition_sequence(self, transition:Transition):
        return self.post_transitions.get(transition, PreOrPostTransitionSequence(transition, 'post'))

    def get_pre_transitions_sequences(self):
        return self.pre_transitions

    def get_post_transitions_sequences(self):
        return self.post_transitions

class JsonFSMConfigParser(FSMConfigParser):

    def __init__(self, config_data, **kwargs):

        super(JsonFSMConfigParser, self).__init__(**kwargs)

        '''
        Takes a config.json describing the FSM, and stores it as a class object
        '''
        self.config_data = config_data

        self.states           = self.config_data['states']
        self.transitions      = self._parse_transitions(self.config_data['transitions'])
        self.initial_state    = self.config_data['initial_state']

        self.interfaces = {}
        for name, data in config_data.get('interfaces', {}).items():
            self.interfaces[name] = FSMInterfaceFactory.get().get_interface(name, data)

        self.pre_transitions_data  = self.config_data.get('pre_transitions', {})
        self.post_transitions_data = self.config_data.get('post_transitions', {})

        self.pre_transitions  = {}
        self.post_transitions = {}

        for prefix in ['pre', 'post']:
            for i, transition in enumerate(self.transitions):
                seq = PreOrPostTransitionSequence(
                    transition,
                    prefix,
                )

                if not self.config_data.get(f'{prefix}_transitions'):
                    if prefix == 'pre':
                        self.pre_transitions[transition] = seq
                    else:
                        self.post_transitions[transition] = seq
                    continue

                blurp = self.config_data[f'{prefix}_transitions'].get(
                    transition.name,
                    {
                        'order':[],
                        'mandatory': []
                    }
                )
                for interface in blurp['order']:
                    seq.add_callback(
                        interface = self.interfaces[interface],
                        mandatory = interface in blurp.get("mandatory", []),
                    )
                self.transitions[i].arguments += seq.get_arguments()

                if prefix == 'pre':
                    self.pre_transitions[transition] = seq
                else:
                    self.post_transitions[transition] = seq

        # for transition in self.transitions:
        #     print(transition.name, transition.arguments, self.pre_transitions[transition], self.post_transitions[transition])

    def _parse_transitions(self, transitions):
        trs = [
            Transition(
                name = t['trigger'],
                source = t['source'],
                destination = t['dest'],
                arguments = [] # for now, no arguments
            )
            for t in transitions
        ]
        return trs


    def get_initial_state(self):
        return self.initial_state

    def get_states(self):
        return self.states

    def get_transitions(self):
        return self.transitions


class FSM:
    def __init__(self, conf, conf_type:ConfTypes):
        match conf_type:
            case ConfTypes.Json:
                self.config = JsonFSMConfigParser(conf)
            case _:
                raise ConfTypeNotSupported(conf_type, 'FSM')

        from logging import getLogger
        self._log = getLogger('FSM')

        self.initial_state = self.config.get_initial_state()
        self.states = self.config.get_states()

        self.transitions = self.config.get_transitions()
        self._log.info('Allowed transitions are:')
        for t in self.transitions:
            self._log.info(str(t))

        self._enusure_unique_transition(self.transitions)

        self._log.info(f'Initial state is "{self.initial_state}"')

        self.pre_transition_sequences = self.config.get_pre_transitions_sequences()
        self.post_transition_sequences = self.config.get_post_transitions_sequences()

    def _enusure_unique_transition(self, transitions):
        a_set = set()
        for t in transitions:
            if t.name in a_set:
                raise fsme.DuplicateTransition(t.name)
            a_set.add(t.name)

    def get_all_states(self) -> List[str]:
        '''
        grabs all the states
        '''
        return self.states

    def get_all_transitions(self) -> List[Transition]:
        '''
        grab all the transitions
        '''
        return self.transitions


    def get_destination_state(self, source_state, transition) -> str:
        '''
        Tells us where a particular transition will take us, given the source_state
        '''
        right_name = [t for t in self.transitions if t == transition]
        for tr in right_name:
            if self.can_execute_transition(source_state, transition):
                return tr.destination

    def get_executable_transitions(self, source_state) -> List[Transition]:
        valid_transitions = []

        for tr in self.transitions:
            debug_txt = f'Testing if transition {str(tr)} is executable from state "{source_state}"...'
            if self.can_execute_transition(source_state, tr):
                self._log.debug(f'{debug_txt} Yes')
                valid_transitions.append(tr)
            else:
                self._log.debug(f'{debug_txt} No\n')

        return valid_transitions


    def get_transition(self, transition_name) -> bool:
        transition = [t for t in self.transitions if t.name == transition_name]
        if not transition:
            fsme.NoTransitionOfName(transition_name)
        return transition[0]


    def can_execute_transition(self, source_state, transition) -> bool:
        '''
        Check that this transition is allowed given the source_state
        '''
        from drunc.utils.utils import regex_match
        self._log.debug(f'can_execute_transition {str(transition.source)} {source_state}')
        return regex_match(transition.source, source_state)


    def prepare_transition(self, transition, transition_data, transition_args):
        transition_data = self.pre_transition_sequences[transition].execute(
            transition_data,
            transition_args
        )
        return transition_data


    def finalise_transition(self, transition, transition_data, transition_args):
        transition_data = self.post_transition_sequences[transition].execute(
            transition_data,
            transition_args,
        )
        return transition_data
