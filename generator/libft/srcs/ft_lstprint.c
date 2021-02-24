/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstprint.c                                      :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/08/31 14:56:31 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:45:39 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	print_content(void *content, unsigned opt)
{
	if (opt == CTYPE)
		ft_printf("content: '%c'\n", *(char *)content);
	else if (opt == STYPE)
		ft_printf("content: '%s'\n", (char *)content);
	else if (opt == DTYPE)
		ft_printf("content: %d\n", *(int *)content);
	else if (opt == UTYPE)
		ft_printf("content: %u\n", *(unsigned *)content);
	else if (opt == OTYPE)
		ft_printf("content: %o\n", *(unsigned *)content);
	else if (opt == XTYPE)
		ft_printf("content: %x\n", *(unsigned *)content);
}

void	ft_lstprint(t_head *head, char *name, unsigned opt)
{
	int		i;
	t_lst	*elem;

	i = 0;
	ft_printf("THIS USES PRINTF\n");
	ft_printf("\nprinting list '%s'\n--\nlist start\n--\n", name);
	ft_printf("head: %p, size: %u, first: %p, last: %p\n"
		, head, head->size, head->first, head->last);
	if (!(elem = head->first))
	{
		ft_printf("--\nlist '%s' is empty\n--\n", name);
		return ;
	}
	ft_printf("first->prev: %p - last->next: %p\n--\n"
		, head->first->prev, head->last->next);
	while (elem)
	{
		ft_printf("link %d: %-14p, prev %-14p, content %-14p\n"
			, ++i, elem, elem->prev, elem->content);
		if (opt)
			print_content(elem->content, opt);
		elem = elem->next;
	}
	ft_printf("--\nlist end\n--\n\n");
}
