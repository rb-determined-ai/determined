# coding: utf-8

"""
    Determined API (Beta)

    Determined helps deep learning teams train models more quickly, easily share GPU resources, and effectively collaborate. Determined allows deep learning engineers to focus on building and training models at scale, without needing to worry about DevOps or writing custom code for common tasks like fault tolerance or experiment tracking.  You can think of Determined as a platform that bridges the gap between tools like TensorFlow and PyTorch --- which work great for a single researcher with a single GPU --- to the challenges that arise when doing deep learning at scale, as teams, clusters, and data sets all increase in size.  # noqa: E501

    OpenAPI spec version: 0.1
    Contact: community@determined.ai
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""


import pprint
import re  # noqa: F401

import six


class V1CheckpointMetadata(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    """
    Attributes:
      swagger_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    swagger_types = {
        'trial_id': 'int',
        'trial_run_id': 'int',
        'uuid': 'str',
        'resources': 'dict(str, str)',
        'framework': 'str',
        'format': 'str',
        'determined_version': 'str',
        'total_batches': 'int',
        'total_records': 'int',
        'total_epochs': 'float'
    }

    attribute_map = {
        'trial_id': 'trialId',
        'trial_run_id': 'trialRunId',
        'uuid': 'uuid',
        'resources': 'resources',
        'framework': 'framework',
        'format': 'format',
        'determined_version': 'determinedVersion',
        'total_batches': 'totalBatches',
        'total_records': 'totalRecords',
        'total_epochs': 'totalEpochs'
    }

    def __init__(self, trial_id=None, trial_run_id=None, uuid=None, resources=None, framework=None, format=None, determined_version=None, total_batches=None, total_records=None, total_epochs=None):  # noqa: E501
        """V1CheckpointMetadata - a model defined in Swagger"""  # noqa: E501

        self._trial_id = None
        self._trial_run_id = None
        self._uuid = None
        self._resources = None
        self._framework = None
        self._format = None
        self._determined_version = None
        self._total_batches = None
        self._total_records = None
        self._total_epochs = None
        self.discriminator = None

        self.trial_id = trial_id
        self.trial_run_id = trial_run_id
        self.uuid = uuid
        if resources is not None:
            self.resources = resources
        self.framework = framework
        self.format = format
        self.determined_version = determined_version
        if total_batches is not None:
            self.total_batches = total_batches
        if total_records is not None:
            self.total_records = total_records
        if total_epochs is not None:
            self.total_epochs = total_epochs

    @property
    def trial_id(self):
        """Gets the trial_id of this V1CheckpointMetadata.  # noqa: E501

        The ID of the trial associated with the checkpoint.  # noqa: E501

        :return: The trial_id of this V1CheckpointMetadata.  # noqa: E501
        :rtype: int
        """
        return self._trial_id

    @trial_id.setter
    def trial_id(self, trial_id):
        """Sets the trial_id of this V1CheckpointMetadata.

        The ID of the trial associated with the checkpoint.  # noqa: E501

        :param trial_id: The trial_id of this V1CheckpointMetadata.  # noqa: E501
        :type: int
        """
        if trial_id is None:
            raise ValueError("Invalid value for `trial_id`, must not be `None`")  # noqa: E501

        self._trial_id = trial_id

    @property
    def trial_run_id(self):
        """Gets the trial_run_id of this V1CheckpointMetadata.  # noqa: E501

        The run of the trial assocaited with the checkpoint.  # noqa: E501

        :return: The trial_run_id of this V1CheckpointMetadata.  # noqa: E501
        :rtype: int
        """
        return self._trial_run_id

    @trial_run_id.setter
    def trial_run_id(self, trial_run_id):
        """Sets the trial_run_id of this V1CheckpointMetadata.

        The run of the trial assocaited with the checkpoint.  # noqa: E501

        :param trial_run_id: The trial_run_id of this V1CheckpointMetadata.  # noqa: E501
        :type: int
        """
        if trial_run_id is None:
            raise ValueError("Invalid value for `trial_run_id`, must not be `None`")  # noqa: E501

        self._trial_run_id = trial_run_id

    @property
    def uuid(self):
        """Gets the uuid of this V1CheckpointMetadata.  # noqa: E501

        UUID of the checkpoint.  # noqa: E501

        :return: The uuid of this V1CheckpointMetadata.  # noqa: E501
        :rtype: str
        """
        return self._uuid

    @uuid.setter
    def uuid(self, uuid):
        """Sets the uuid of this V1CheckpointMetadata.

        UUID of the checkpoint.  # noqa: E501

        :param uuid: The uuid of this V1CheckpointMetadata.  # noqa: E501
        :type: str
        """
        if uuid is None:
            raise ValueError("Invalid value for `uuid`, must not be `None`")  # noqa: E501

        self._uuid = uuid

    @property
    def resources(self):
        """Gets the resources of this V1CheckpointMetadata.  # noqa: E501

        Dictionary of file paths to file sizes in bytes of all files.  # noqa: E501

        :return: The resources of this V1CheckpointMetadata.  # noqa: E501
        :rtype: dict(str, str)
        """
        return self._resources

    @resources.setter
    def resources(self, resources):
        """Sets the resources of this V1CheckpointMetadata.

        Dictionary of file paths to file sizes in bytes of all files.  # noqa: E501

        :param resources: The resources of this V1CheckpointMetadata.  # noqa: E501
        :type: dict(str, str)
        """

        self._resources = resources

    @property
    def framework(self):
        """Gets the framework of this V1CheckpointMetadata.  # noqa: E501

        The framework associated with the checkpoint.  # noqa: E501

        :return: The framework of this V1CheckpointMetadata.  # noqa: E501
        :rtype: str
        """
        return self._framework

    @framework.setter
    def framework(self, framework):
        """Sets the framework of this V1CheckpointMetadata.

        The framework associated with the checkpoint.  # noqa: E501

        :param framework: The framework of this V1CheckpointMetadata.  # noqa: E501
        :type: str
        """
        if framework is None:
            raise ValueError("Invalid value for `framework`, must not be `None`")  # noqa: E501

        self._framework = framework

    @property
    def format(self):
        """Gets the format of this V1CheckpointMetadata.  # noqa: E501

        The format of the checkpoint.  # noqa: E501

        :return: The format of this V1CheckpointMetadata.  # noqa: E501
        :rtype: str
        """
        return self._format

    @format.setter
    def format(self, format):
        """Sets the format of this V1CheckpointMetadata.

        The format of the checkpoint.  # noqa: E501

        :param format: The format of this V1CheckpointMetadata.  # noqa: E501
        :type: str
        """
        if format is None:
            raise ValueError("Invalid value for `format`, must not be `None`")  # noqa: E501

        self._format = format

    @property
    def determined_version(self):
        """Gets the determined_version of this V1CheckpointMetadata.  # noqa: E501

        The Determined version associated with the checkpoint.  # noqa: E501

        :return: The determined_version of this V1CheckpointMetadata.  # noqa: E501
        :rtype: str
        """
        return self._determined_version

    @determined_version.setter
    def determined_version(self, determined_version):
        """Sets the determined_version of this V1CheckpointMetadata.

        The Determined version associated with the checkpoint.  # noqa: E501

        :param determined_version: The determined_version of this V1CheckpointMetadata.  # noqa: E501
        :type: str
        """
        if determined_version is None:
            raise ValueError("Invalid value for `determined_version`, must not be `None`")  # noqa: E501

        self._determined_version = determined_version

    @property
    def total_batches(self):
        """Gets the total_batches of this V1CheckpointMetadata.  # noqa: E501

        The number of batches trained on when these metrics were reported.  # noqa: E501

        :return: The total_batches of this V1CheckpointMetadata.  # noqa: E501
        :rtype: int
        """
        return self._total_batches

    @total_batches.setter
    def total_batches(self, total_batches):
        """Sets the total_batches of this V1CheckpointMetadata.

        The number of batches trained on when these metrics were reported.  # noqa: E501

        :param total_batches: The total_batches of this V1CheckpointMetadata.  # noqa: E501
        :type: int
        """

        self._total_batches = total_batches

    @property
    def total_records(self):
        """Gets the total_records of this V1CheckpointMetadata.  # noqa: E501

        The number of batches trained on when these metrics were reported.  # noqa: E501

        :return: The total_records of this V1CheckpointMetadata.  # noqa: E501
        :rtype: int
        """
        return self._total_records

    @total_records.setter
    def total_records(self, total_records):
        """Sets the total_records of this V1CheckpointMetadata.

        The number of batches trained on when these metrics were reported.  # noqa: E501

        :param total_records: The total_records of this V1CheckpointMetadata.  # noqa: E501
        :type: int
        """

        self._total_records = total_records

    @property
    def total_epochs(self):
        """Gets the total_epochs of this V1CheckpointMetadata.  # noqa: E501

        The number of epochs trained on when these metrics were reported.  # noqa: E501

        :return: The total_epochs of this V1CheckpointMetadata.  # noqa: E501
        :rtype: float
        """
        return self._total_epochs

    @total_epochs.setter
    def total_epochs(self, total_epochs):
        """Sets the total_epochs of this V1CheckpointMetadata.

        The number of epochs trained on when these metrics were reported.  # noqa: E501

        :param total_epochs: The total_epochs of this V1CheckpointMetadata.  # noqa: E501
        :type: float
        """

        self._total_epochs = total_epochs

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(V1CheckpointMetadata, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, V1CheckpointMetadata):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other