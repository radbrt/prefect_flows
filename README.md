# Prefect flows
This repo contains some Prefect flows that can be automatically deployed via Github Actions.

Both the flows and the storage is docker-based - keeping everything in one place.

Under the `projects` folder are subfolders that correspond to projects in Prefect. Before a new folder under `projects/` can be made, create a Prefect project with the same name. Most things are automated, but not this.

Under the specific folders, there are folders that can correspond to specific flows. It is perfectly possible to have several flows under the same subfolder, but by design one subfolder is self-contained containing a `requirements.txt` file and possibly a `Dockerfile` to build custom images. For better or worse, the subfolder needs to contain a `requirements.txt` file even if it is empty - or the github action will fail.


This flow has a few pros and cons. On the plus side:

- The flows are almost perfectly independent, and can built on different images with different and potentially incompatible programs and libraries.
- It follows a template, so that new flows can be created simply by copying the existing boilerplate and focusing on the logic of the flow.
- Deploying is done through the github actions file, no custom python code necessary.
- Flows are registered in parallel.
- It is possible to redeploy all flows, which will rebuild and potentially update the docker images - a big security win.

On the minus side:
- Although the github actions file is fairly short, it contains a few bits that are hard to read.
- For new developers, there is a lot of boilerplate.
- Because the flows are independent, every flow generates a docker image. The container registry might get big.

This particular example does not contain dev/test/prod references. For users with different tenants, it is possible to create several github action files triggering on different branches. For single-tenant users, it is possible repurpose the projects to reflect dev/test/prod environments. Once again, different github actions would trigger on different branches.

So far, there is only one trigger: `workflow_dispatch` - meaning that we have to trigger it manually. Ideally, we might want to trigger it periodically, rebuilding and updating the images regularaly. This would reregister the flow, which seems to increment the flow version even though nothing has changed.