
#Credit: # Base cleaner.py code from https://github.com/CyberZHG Git repo:
# https://github.com/CyberZHG/wiki-dump-reader/blob/master/wiki_dump_reader/cleaner.py
import re


def _remove_resource_links(text, resource):
    """Remove links likes `[[*:*]]`"""
    pattern = '[[' + resource + ':'
    pattern_begin = text.find(pattern)
    if pattern_begin == -1:
        return text
    begin, removed = 0, ''
    while begin < len(text):
        if pattern_begin > begin:
            removed += text[begin:pattern_begin]
        pattern_end, depth = pattern_begin + 2, 2
        while pattern_end < len(text):
            ch = text[pattern_end]
            pattern_end += 1
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    break
        if depth == 0:
            begin = pattern_end
        else:
            removed += text[begin]
            begin += 1
        pattern_begin = text.find(pattern, begin)
        if pattern_begin == -1:
            break
    if len(text) > begin:
        removed += text[begin:]
    return removed


def _remove_file_links(text):
    """Remove links like `[[File:*]]`"""
    text = _remove_resource_links(text, 'File')
    text = re.sub(r'^File:.*$', '', text, flags=re.MULTILINE)
    return text


def _remove_image_links(text):
    """Remove links like `[[File:*]]`"""
    return _remove_resource_links(text, 'Image')


def _remove_external_links(text):
    """Remove links like [*]"""
    return re.sub(r'\[h[^ ]+ (.*?)\]', r'\1', text)


def _remove_refs(text):
    """Remove patterns like <ref*>*</ref>"""
    text = re.sub(r'<ref[^/]*?/>', '', text,
                  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<ref.*?</ref>', '', text,
                  flags=re.IGNORECASE | re.DOTALL)
    return text


def _remove_emphasises(text):
    """Remove patterns like '''*'''"""
    text = re.sub(r"'''(.*?)'''", r'\1', text, flags=re.DOTALL)
    text = re.sub(r"''(.*?)''", r'\1', text, flags=re.DOTALL)
    return text


def _remove_comments(text):
    """Remove patterns like <!--*-->"""
    return re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)


def _remove_langs(text):
    """Remove pattenrs like {{lang-*|*}}}"""
    return re.sub(r'{{lang(-|\|).*?\|(.*?)}}', r'\2', text, flags=re.IGNORECASE | re.DOTALL)


def _remove_titles(text):
    """Remove patterns like ==*=="""
    return re.sub(r'(={2,6})\s*(.*?)\s*\1', r'\2', text)


def _remove_choices(text):
    """Remove patterns like -{zh-hans:*; zh-hant:*}-"""
    text = re.sub(
        r'-{.{,100}?zh(-hans|-cn|-hk|):(.{,100}?)(;.{,100}?}-|}-)',
        r'\2', text,
        flags=re.DOTALL
    )
    text = re.sub(r'-{.{,100}?:(.{,100}?)(;.{,100}?}-|}-)',
                  r'\1', text, flags=re.DOTALL)
    text = re.sub(r'-{(.{,100}?)}-', r'\1', text, flags=re.DOTALL)
    return text


def _remove_templates(text):
    """Remove patterns like {{*}}"""
    begin, removed = 0, ''
    while begin < len(text):
        pattern_begin = text.find('{{', begin)
        if pattern_begin == -1:
            if begin == 0:
                removed = text
            else:
                removed += text[begin:]
            break
        if pattern_begin > begin:
            removed += text[begin:pattern_begin]
        pattern_end, depth = pattern_begin + 2, 2
        while pattern_end < len(text):
            ch = text[pattern_end]
            pattern_end += 1
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    link = text[pattern_begin + 2:pattern_end - 2]
                    parts = link.split('|')
                    template_type = parts[0].split(' ')[0].lower()
                    if len(parts) == 1:
                        if all(map(lambda x: x in {'"', "'", ' '}, parts[0][:])):
                            removed += parts[0].replace(' ', '')
                    elif len(parts) in [2, 3]:
                        if template_type in {'le'} or template_type.startswith('link-'):
                            removed += parts[1]
                    break
        begin = pattern_end
    return removed


def _remove_htmls(text):
    return re.sub(r'<(.*?)>', '', text, flags=re.DOTALL)


def _remove_lists(text):
    return re.sub(r'^\s*[\*#]\s*', '', text, flags=re.MULTILINE)


def _remove_indents(text):
    return re.sub(r'^\s*[:;]\s*', '', text, flags=re.MULTILINE)


def _remove_styles(text):
    return re.sub(r':?{\| (style|class)=.*?\|}', '', text, flags=re.IGNORECASE | re.DOTALL)


def _remove_spaces(text):
    return text.replace('\u200b', '')


def _remove_continuous_newlines(text):
    return re.sub(r'\n{2,}', '\n', text)


def build_links(text):
    begin, removed, links = 0, '', []
    while begin < len(text):
        pattern_begin = text.find('[[', begin)
        if pattern_begin == -1:
            if begin == 0:
                removed = text
            else:
                removed += text[begin:]
            break
        if pattern_begin > begin:
            removed += text[begin:pattern_begin]
        pattern_end, depth = pattern_begin + 2, 2
        while pattern_end < len(text):
            ch = text[pattern_end]
            pattern_end += 1
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    link = text[pattern_begin + 2:pattern_end - 2]
                    parts = link.split('|')
                    if len(parts) == 1:
                        if ':' in link:
                            pure = link.split(':')[-1]
                            links.append({
                                'begin': len(removed),
                                'end': len(removed) + len(pure),
                                'link': link,
                                'text': pure
                            })
                            removed += pure
                        else:
                            links.append({
                                'begin': len(removed),
                                'end': len(removed) + len(text),
                                'link': link,
                                'text': link
                            })
                            removed += link
                    elif len(parts) == 2:
                        links.append({
                            'begin': len(removed),
                            'end': len(removed) + len(parts[1]),
                            'link': parts[0],
                            'text': parts[1]
                        })
                        removed += parts[1]
                    break
        begin = pattern_end
    return removed.strip(), links


def clean_text(text):
    text = _remove_file_links(text)
    text = _remove_image_links(text)
    text = _remove_external_links(text)
    text = _remove_refs(text)
    text = _remove_emphasises(text)
    text = _remove_comments(text)
    text = _remove_langs(text)
    text = _remove_choices(text)
    text = _remove_templates(text)
    text = _remove_htmls(text)
    text = _remove_lists(text)
    text = _remove_indents(text)
    text = _remove_styles(text)
    text = _remove_spaces(text)
    text = _remove_continuous_newlines(text)
    return text.strip()
