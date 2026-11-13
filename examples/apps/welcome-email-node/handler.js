export function handler(event, context) {
  const user = event.newImage

  console.log(
    `Sending welcome email to ${user.email} (request ${context.requestId})`,
  )
}
