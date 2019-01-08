#!groovy

@Library('sec_ci_libs@v2-latest') _

ansiColor('xterm') {

  node("mesos-ec2-ubuntu-16.04-raidmod") {
    properties([
      parameters([
        string(name: "SLACK_CREDENTIAL_ID", defaultValue: "25fe61e8-597e-430f-94bf-e58df726f9eb"),
        string(name: "SLACK_CHANNEL", defaultValue: "#dcos-storage"),
        string(name: "ALERTS_FOR_BRANCHES", defaultValue: "master")
      ])
    ])

    stage("Verify author") {
      def alerts_for_branches = params.ALERTS_FOR_BRANCHES.tokenize(",") as String[]
      user_is_authorized(alerts_for_branches, params.SLACK_CREDENTIAL_ID, params.SLACK_CHANNEL)
    }
  }

  node("mesos-ec2-ubuntu-16.04-raidmod") {
    properties([
      parameters([
        string(name: "GITHUB_CREDENTIAL_ID", defaultValue: "d146870f-03b0-4f6a-ab70-1d09757a51fc"),
        string(name: "S3_REGION", defaultValue: "us-east-1"),
        string(name: "S3_BUCKET_CRED_ID", defaultValue: "e15e75be-8686-4ad2-8b46-fe7fa9fcae54"),
        string(name: "AWS_PROFILE_NAME_CRED_ID", defaultValue: "7e66a13f-51ca-4bb2-9b42-340f3f089118")
      ])
    ])

    checkout scm

    def packageSHA = getPackageSHA()
    def packageVersion = getPackageVersion()

    stage("Prepare") {
      sh("make rebuild-dev-image")
    }

    stage("Build and Test") {
      withEnv(["DOCKER=yes","PACKAGE_SHA=${packageSHA}","PLUGIN_VERSION=${packageVersion}"]) {
        sh("make check")
        sh("make")

        lock(label: "linux-dev-loop") {
          timeout(30) {
            sh("make sudo-test")
          }
        }
      }
    }

    stage("Publish") {
      def isPullRequest = (env.CHANGE_ID != null)
      def isRelease = (packageSHA != packageVersion)

      archiveArtifacts(artifacts: "csilvm")

      withCredentials([
          string(credentialsId: params.S3_BUCKET_CRED_ID, variable: "S3_BUCKET"),
          string(credentialsId: params.AWS_PROFILE_NAME_CRED_ID, variable: "AWS_PROFILE_NAME"),
      ]) {
        def s3path= ""
        if (isRelease) {
          s3path = "${env.S3_BUCKET}/csilvm/build/tag/${packageVersion}"
        } else {
          s3path = "${env.S3_BUCKET}/csilvm/build/sha/${packageSHA}"
        }

        publishToS3(s3path, false, "csilvm", env.AWS_PROFILE_NAME)

        if (!isPullRequest && !isRelease) {
          publishToS3("${env.S3_BUCKET}/csilvm/build/latest", true, "csilvm", env.AWS_PROFILE_NAME)
        }
      }
    }
  }
}

def publishToS3(String bucket, Boolean keepForever, String sourceFile, String awsProfileName) {
  step([
      $class: "S3BucketPublisher",
      entries: [[
          selectedRegion: params.S3_REGION,
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
      profileName: awsProfileName,
      dontWaitForConcurrentBuildCompletion: false,
      consoleLogLevel: "INFO",
      pluginFailureResultConstraint: "FAILURE"
  ])
}

def getPackageSHA() {
  def isPullRequest = (env.CHANGE_ID != null)

  if (isPullRequest) {
    def parents = sh(
          returnStdout: true,
          script: "git log --pretty=%P -n 1 HEAD").trim().split()

    if (parents.size() != 1) {
      // Non fast-forward case.
      return parents[0]
    }
  }

  return sh(
      returnStdout: true,
      script: "git rev-parse HEAD").trim()
}

def getPackageVersion() {
  def packageSHA = getPackageSHA()

  return sh(
      returnStdout: true,
      script: "git describe --exact-match ${packageSHA} 2>/dev/null || echo ${packageSHA}").trim()
}
