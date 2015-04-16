# -*- coding: utf-8 -*-

"""
$Id: $
"""

from django import forms
from django.forms.widgets import Input
from django.core import validators
from django.utils.translation import ugettext_lazy as _D
from rx.i18n import _


class FileInput(Input):
    '''CherryPy-compatible file widget'''
    input_type = 'file'
    needs_multipart_form = True

    def render(self, name, value, attrs=None):
        return super(FileInput, self).render(name, None, attrs=attrs)

    def value_from_datadict(self, data, files, name):
        return data.get(name, None)

    def _has_changed(self, initial, data):
        if data is None:
            return False
        return True


class FileField(forms.Field):
    '''CherryPy-compatible file field'''
    widget = FileInput
    default_error_messages = {
        'invalid': _D(u"No file was submitted. Check the encoding type on the form."),
        'missing': _D(u"No file was submitted."),
        'empty': _D(u"The submitted file is empty."),
    }

    def __init__(self, *args, **kwargs):
        self.valid_extensions = kwargs.pop('valid_extensions', None)
        self.content_types = kwargs.pop('content_types', None)
        self.max_upload_size = kwargs.pop('max_upload_size', None)
        super(FileField, self).__init__(*args, **kwargs)

    def to_python(self, data):
        if data in validators.EMPTY_VALUES:
            return None

        file_name = data.filename
        file_size = data.length

        if not file_name:
            raise forms.ValidationError(self.error_messages['invalid'])
        if not file_size:
            raise forms.ValidationError(self.error_messages['empty'])

        if self.valid_extensions is not None:
            ext = os.path.splitext(file_name)[1][1:] # strip dot from extension
            if ext not in self.valid_extensions:
                raise forms.ValidationError(_(u'Недопустимое расширение файла'))

        content_type = data.type
        if (self.content_types is not None) and (content_type not in self.content_types):
            raise forms.ValidationError(_(u'Недопустимый тип файла'))

        if (self.max_upload_size is not None) and (file_size > self.max_upload_size):
            raise forms.ValidationError(
                _(u'Максимально допустимый размер файла - %s. Вы попытались загрузить файл размером %s') % (
                    filesizeformat(self.max_upload_size),
                    filesizeformat(file_size)
                )
            )

        return data

    def clean(self, data, initial=None):
        if data is None and initial:
            return initial
        return super(FileField, self).clean(data)

