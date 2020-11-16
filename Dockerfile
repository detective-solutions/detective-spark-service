FROM openjdk:8-jdk

# Configure apt
ENV DEBIAN_FRONTEND=noninteractive

# Mini Conda Start
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH
RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc


RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 ca-certificates curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate dbconnect" >> ~/.bashrc


ENV TINI_VERSION v0.16.1
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

COPY . /tmp/
RUN pip install --upgrade pip \
    && conda env create -f /tmp/environment7.1.1.yml \
    && echo '{}' > /root/.databricks-connect

# Clean up
RUN apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

ENV CONDA_DEFAULT_ENV="dbconnect"
ENV DEBIAN_FRONTEND=dialog
ENV DATABRICKS_ADDRESS='https://adb-4677518036746641.1.azuredatabricks.net/'
ENV DATABRICKS_API_TOKEN='dapif3799217eedca9517d9715913d8584f8'
ENV DATABRICKS_CLUSTER_ID='1101-170914-adopt980'
ENV DATABRICKS_ORG_ID=4677518036746641
ENV DATABRICKS_PORT=8787

ENV PATH=~/anaconda3/bin:$PATH


# Allow for a consistant java home location for settings - image is changing over time
RUN if [ ! -d "/docker-java-home" ]; then ln -s "${JAVA_HOME}" /docker-java-home; fi

WORKDIR /tmp

CMD ["/bin/bash", "docker-entrypoint.sh"]
