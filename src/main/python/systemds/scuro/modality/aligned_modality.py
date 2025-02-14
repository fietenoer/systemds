# -------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------
from typing import List

from systemds.scuro.modality.modality import Modality
from systemds.scuro.representations.fusion import Fusion


class AlignedModality(Modality):
    def __init__(self, representation: Fusion, modalities: List[Modality]):
        """
        Defines the modality that is created during the fusion process
        :param representation: The representation for the aligned modality
        :param modalities: List of modalities to be combined
        """
        name = ""
        for modality in modalities:
            name += modality.name
        super().__init__(representation, modality_name=name)
        self.modalities = modalities

    def combine(self):
        """
        Initiates the call to fuse the given modalities depending on the Fusion type
        """
        self.data = self.representation.fuse(self.modalities)  # noqa

    def get_modality_names(self):
        names = []
        for modality in self.modalities:
            names.append(modality.name)

        return names
