/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_memmove.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/04 20:37:27 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:14:30 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

void	*ft_memmove(void *dst, const void *src, size_t len)
{
	size_t		pos;

	pos = 0;
	if (src < dst)
	{
		while (len)
		{
			*(unsigned char *)(dst + len - 1) = *(unsigned char *)(src + len
				- 1);
			len--;
		}
	}
	else
	{
		while (pos < len)
		{
			*(unsigned char *)(dst + pos) = *(unsigned char *)(src + pos);
			pos++;
		}
	}
	return (dst);
}
