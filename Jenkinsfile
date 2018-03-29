#!groovy

import groovy.json.JsonOutput

// The following environemnt variables must be set accordingly:
// 1. NODE_LABELS (string)
// 2. GITHUB_CREDENTIAL_ID (string)
// 3. S3_REGION (string)
// 4. S3_BUCKET (string)
// 5. AWS_PROFILE_NAME (string)

node(env.NODE_LABELS) {
  checkout scm

  stage("Prepare") {
    sh("make rebuild-dev-image")
  }

  stage("Build and Test") {
    sh("DOCKER=yes make check")
    sh("DOCKER=yes make")

    lock(label: "linux-dev-loop") {
      sh("DOCKER=yes make sudo-test")
    }
  }

  stage("Publish") {
    def packageSHA = env.ghprbActualCommit
    if (packageSHA == null) {
      packageSHA = sh(
          returnStdout: true,
          script: "git rev-parse HEAD").trim()
    }

    def packageVersion = sh(
        returnStdout: true,
        script: "git describe --exact-match ${packageSHA} 2>/dev/null || echo ${packageSHA}").trim()

    def isPullRequest = (env.ghprbPullId != null)
    def isRelease = (packageSHA != packageVersion)

    archiveArtifacts(artifacts: "csilvm")

    def s3path= ""
    if (isRelease) {
      s3path = "${env.S3_BUCKET}/csilvm/build/tag/${packageVersion}"
    } else {
      s3path = "${env.S3_BUCKET}/csilvm/build/sha/${packageSHA}"
    }

    publishToS3(s3path, false, "csilvm")

    if (!isPullRequest && !isRelease) {
      publishToS3("${env.S3_BUCKET}/csilvm/build/latest", true, "csilvm")
    }

    if (isPullRequest) {
      postGithubIssueComments(
          env.ghprbPullId,
          """\
          Success! Download the [plugin](https://s3.amazonaws.com/${env.S3_BUCKET}/csilvm/build/sha/${packageSHA}/csilvm) (SHA: ${packageSHA})
          """.stripIndent())
    }
  }
}

def publishToS3(String bucket, Boolean keepForever, String sourceFile) {
  step([
      $class: "S3BucketPublisher",
      entries: [[
          selectedRegion: env.S3_REGION,
          bucket: bucket,
          sourceFile: sourceFile,
          storageClass: "STANDARD",
          noUploadOnFailure: true,
          uploadFromSlave: true,
          managedArtifacts: false,
          flatten: false,
          showDirectlyInBrowser: false,
          keepForever: keepForever
      ]],
      profileName: env.AWS_PROFILE_NAME,
      dontWaitForConcurrentBuildCompletion: false,
      consoleLogLevel: "INFO",
      pluginFailureResultConstraint: "FAILURE"
  ])
}

def postGithubIssueComments(issue, body) {
  withCredentials([string(credentialsId: env.GITHUB_CREDENTIAL_ID, variable: "GITHUB_TOKEN")]) {
    def payload = JsonOutput.toJson(["body": body])
    def url = "https://api.github.com/repos/mesosphere/csilvm/issues/${issue}/comments"
    def headers = [
        "Authorization": "Token ${env.GITHUB_TOKEN}",
        "Accept": "application/json",
        "Content-type": "application/json"
    ]

    def command = "curl -s -X POST -d '${payload}' "
    headers.each {
      command += "-H '${it.key}: ${it.value}' "
    }
    command += url

    docker.image("appropriate/curl").inside {
      sh(command)
    }
  }
}
