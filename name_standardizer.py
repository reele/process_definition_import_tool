
import re

DEFAULT_THEME='MANUAL'

class SegmentPick:
    def __init__(self, regex: str, default_value: str = None, is_match_partial: bool = True, pick_start: int = None, pick_end: int = None) -> None:
        self.pattern = re.compile(regex)
        self.is_match_partial = is_match_partial
        self.pick_start = int(pick_start) if pick_start is not None else None
        self.pick_end = int(pick_end) if pick_end is not None else None
        self.default_value = default_value

    def action(self, input_str):
        if self.is_match_partial:
            match = self.pattern.search(input_str)
        else:
            match = self.pattern.fullmatch(input_str)

        if match:
            segment = input_str[match.start():match.end()]

            if self.pick_start is not None and self.pick_end is not None:
                return segment[self.pick_start:self.pick_end]
            elif self.pick_start is not None:
                return segment[self.pick_start:]
            elif self.pick_end is not None:
                return segment[:self.pick_end]
            else:
                return segment

        return None


class SegmentCut:
    def __init__(self, regex: str) -> None:
        self.pattern = re.compile(regex)

    def action(self, input_str):
        match = self.pattern.search(input_str)
        if match:
            return input_str[:match.start()] + input_str[match.end():]
        else:
            return None


class NameStandardizeRule:
    def __init__(self) -> None:
        self.rules: dict[str, list] = {}
    
    def add_rule(self, rule_type, rule_action):
        rule_list = self.rules.get(rule_type)
        if rule_list is None:
            rule_list = []
            self.rules[rule_type] = rule_list
        rule_list.append(rule_action)

    def action(self, rule_type, input_str, default):

        rule_list = self.rules.get(rule_type)
        if not rule_list:
            return input_str
        
        for rule in rule_list:
            result = rule.action(input_str)
            if result:
                break
        else:
            result = default

        return result


class NameStandardizer:
    def __init__(self) -> None:
        self.group_rule_mapping: dict[str, NameStandardizeRule] = {}

    def add_group_rule(self, group, rule_type, rule_action):
        rule = self.group_rule_mapping.get(group)
        if rule is None:
            rule = NameStandardizeRule()
            self.group_rule_mapping[group] = rule
        
        rule.add_rule(rule_type, rule_action)
    
    def standardize(self, group: str, input_str: str):
        mapping = self.group_rule_mapping.get(group)
        if mapping:
            cut_result = self.group_rule_mapping[group].action('CUT', input_str, input_str)
            theme = self.group_rule_mapping[group].action('THEME', cut_result, DEFAULT_THEME)
            sub_theme = self.group_rule_mapping[group].action('SUB_THEME', cut_result, theme)

            return (cut_result, theme, sub_theme)
        else:
            return (input_str, '', '')
    
    def standardize_full(self, group: str, input_str: str):
        cut_result, theme, sub_theme = self.standardize(group, input_str)
        full_name = '{}_{}'.format(group, cut_result)
        return (cut_result, theme, sub_theme, full_name)

